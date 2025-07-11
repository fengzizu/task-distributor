package com.zfq.common.taskdistributor.pipeline;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class PipeLine<T> implements ApplicationRunner, ApplicationContextAware, SmartLifecycle {
    protected final Collection<String> inputTopics;
    protected final String groupId;
    protected final MessageProcessor<T> messageProcessor;
    protected final int processorCount;
    protected final String containerFactoryName;
    protected final ErrorHandler errorHandler;
    protected final int retryTimes;
    protected final SuccessHandler<T> successHandler;
    protected final int queueSize;
    protected final ThreadPoolExecutor workers;

    /**
     * in order to not use synchronized collections need to operate this using a single thread pool
     */
    protected final Map<TopicPartition, LinkedList<OffsetInfo>> offsetCache;

    /**
     * consumer is not thread safe so need a single thread pool to use consumer
     */
    protected final ScheduledExecutorService consumerThread;

    protected ApplicationContext applicationContext;

    /**
     * single thread pool to operate offset cache
     */
    protected ExecutorService offsetManager;
    private Consumer consumer;
    private volatile boolean isRunning = false;

    protected Duration pollTimeout;

    private ScheduledFuture consumeTask;

    public PipeLine(Collection<String> inputTopics, String groupId, MessageProcessor messageProcessor,
                    int processorCount, int queueSize, String containerFactoryName, ErrorHandler errorHandler,
                    int retryTimes, SuccessHandler successHandler) {
        this.inputTopics = inputTopics;
        this.groupId = groupId;
        this.messageProcessor = messageProcessor;
        this.processorCount = processorCount;
        this.queueSize = queueSize;
        this.containerFactoryName = containerFactoryName;
        this.errorHandler = errorHandler;
        this.retryTimes = retryTimes;
        this.successHandler = successHandler;
        consumerThread = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory(inputTopics + groupId + "-c-"));
        BlockingQueue<Runnable> workQueue = this.queueSize > 0 ? new LinkedBlockingQueue<>(this.queueSize) : new SynchronousQueue<>(true);
        workers = new ThreadPoolExecutor(this.processorCount, this.processorCount, Integer.MAX_VALUE,
                TimeUnit.DAYS, workQueue,
                new CustomizableThreadFactory(inputTopics + groupId + "-worker"), new ThreadPoolExecutor.CallerRunsPolicy());
        offsetCache = new HashMap<>(16);
        offsetManager = Executors.newSingleThreadExecutor(new CustomizableThreadFactory(inputTopics + groupId + "-offset"));
    }

    private void tryCommitOffset() {
        try {
            Map<TopicPartition, OffsetAndMetadata> commit = findNeedCommitOffset();
            if (commit.size() > 0) {
                this.consumer.commitSync(commit);
                offsetManager.execute(() -> {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : commit.entrySet()) {
                        TopicPartition topicPartition = entry.getKey();
                        OffsetAndMetadata offsetMeta = entry.getValue();
                        log.debug("committed topic {} partition {} offset {}", topicPartition.topic(), topicPartition.partition(), offsetMeta.offset() - 1);
                        LinkedList<OffsetInfo> offsetInfoList = offsetCache.get(topicPartition);
                        while (offsetInfoList.size() > 0 && offsetInfoList.getFirst().getOffset() < offsetMeta.offset()) {
                            offsetInfoList.pop();
                        }
                    }
                });
            }
        } catch (Throwable e) {
            log.error("error commit offset", e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> findNeedCommitOffset() {
        try {
            return CompletableFuture.supplyAsync(() -> {
                Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
                for (Map.Entry<TopicPartition, LinkedList<OffsetInfo>> entry : offsetCache.entrySet()) {
                    TopicPartition key = entry.getKey();
                    LinkedList<OffsetInfo> offsetInfoList = entry.getValue();
                    for (OffsetInfo offsetInfo : offsetInfoList) {
                        if (offsetInfo.isDone()) {
                            /**
                             *  @see KafkaConsumer#commitSync(Map)
                             */
                            commit.put(key, new OffsetAndMetadata(offsetInfo.getOffset() + 1));
                        } else {
                            break;
                        }
                    }
                }
                return commit;
            }, offsetManager).get(1, TimeUnit.MINUTES);
        } catch (Throwable e) {
            log.error("error find need commit offset", e);
        }
        return Collections.emptyMap();
    }

    private void initOffset(ConsumerRecord consumerRecord) {
        offsetManager.execute(() -> {
            TopicPartition key = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            List<OffsetInfo> offsetInfoList = offsetCache.computeIfAbsent(key, k -> new LinkedList<>());
            offsetInfoList.add(new OffsetInfo(consumerRecord.offset()));
        });
    }

    protected void process(ConsumerRecord consumerRecord, int retryTimes) {
        CompletableFuture.runAsync(() -> {
            try {
                CompletableFuture future = messageProcessor.process(consumerRecord);
                future.whenCompleteAsync((sr, e) -> {
                    if (e == null) {
                        successHandler.handleSuccess(consumerRecord, (T) sr);
                        submitOffset(consumerRecord);
                    } else {
                        handleError(consumerRecord, (Throwable) e, retryTimes);
                    }
                    log.debug("processed topic {} partition {} offset {} retry times {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), retryTimes);
                }, workers).orTimeout(10, TimeUnit.MINUTES);
            } catch (Throwable e) {
                log.error("process error topic {} partition {} offset {} retry times {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), retryTimes, e);
                handleError(consumerRecord, e, retryTimes);
            }
        }, workers);
    }

    protected void handleError(ConsumerRecord consumerRecord, Throwable ex, int retryTimes) {
        try {
            if (retryTimes < this.retryTimes) {
                process(consumerRecord, retryTimes + 1);
            } else {
                onError(consumerRecord, ex);
                submitOffset(consumerRecord);
            }
        } catch (Throwable e) {
            log.error("error handle error topic {} partition {} offset {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
        }
    }

    protected void onError(ConsumerRecord consumerRecord, Throwable throwable) {
        errorHandler.handleError(consumerRecord, throwable);
    }

    protected void submitOffset(ConsumerRecord consumerRecord) {
        offsetManager.execute(() -> {
            try {
                TopicPartition partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                List<OffsetInfo> offsetInfoList = offsetCache.get(partition);
                if (offsetInfoList != null) {
                    offsetInfoList.stream().filter(offsetInfo -> offsetInfo.getOffset().equals(consumerRecord.offset()))
                            .findFirst().ifPresent(offsetInfo -> offsetInfo.setDone(true));
                }
                log.debug("submitted topic {} partition {} offset {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            } catch (Throwable e) {
                log.error("submit offset error topic {} partition {} offset {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
            }
        });
    }

    protected void removeOffsetCache(TopicPartition topicPartition) {
        offsetManager.execute(() -> offsetCache.remove(topicPartition));
    }

    protected void initOffsetCache(TopicPartition topicPartition) {
        offsetManager.execute(() -> offsetCache.put(topicPartition, new LinkedList<>()));
    }

    private void consumeMessage() {
        try {
            if (approximateAvailableTaskSlot() > 0) {
                ConsumerRecords consumerRecords = consumer.poll(pollTimeout);
                Iterator<ConsumerRecord> iterator = consumerRecords.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord consumerRecord = iterator.next();
                    initOffset(consumerRecord);
                    process(consumerRecord, 0);
                }
            }
            tryCommitOffset();
        } catch (Throwable e) {
            log.error("error consuming message for topic {}", inputTopics, e);
        }
    }

    private int approximateAvailableTaskSlot() {
        return this.processorCount - workers.getActiveCount() + this.queueSize - workers.getQueue().size();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void start() {
        isRunning = true;
    }

    @Override
    public void stop() {
        consumerThread.execute(() -> consumer.close());
        consumerThread.shutdown();
        workers.shutdown();
        isRunning = false;
        log.info("graceful shutdown consumer for topic {} groupId {}", inputTopics, groupId);
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void run(ApplicationArguments args) {
        AbstractKafkaListenerContainerFactory factory = (AbstractKafkaListenerContainerFactory) applicationContext.getBean(this.containerFactoryName);
        ConsumerFactory consumerFactory = factory.getConsumerFactory();
        String maxPollInterval = factory.getContainerProperties().getKafkaConsumerProperties().getProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                String.valueOf(Duration.ofMinutes(5).toMillis()));
        factory.getContainerProperties().getKafkaConsumerProperties().setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval);
        pollTimeout = Duration.ofMillis(Long.valueOf(maxPollInterval) / 5);
        this.consumer = consumerFactory.createConsumer(groupId, null);
        this.consumer.subscribe(inputTopics, new BalanceListener());
        startConsumer();
    }

    private void startConsumer() {
        consumeTask = consumerThread.scheduleAtFixedRate(() -> consumeMessage(), 1, 1, TimeUnit.SECONDS);
    }

    private static class OffsetInfo {
        private Long offset;
        private boolean done = false;

        public OffsetInfo(Long offset) {
            this.offset = offset;
        }

        public Long getOffset() {
            return offset;
        }

        public boolean isDone() {
            return done;
        }

        public void setDone(boolean done) {
            this.done = done;
        }
    }

    private class BalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                removeOffsetCache(partition);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                initOffsetCache(partition);
            }
        }
    }

    /**
     * if topic partitions is empty then seek all topic partitions assigned to this consumer to begin
     */
    public void seekToBegin(Collection<TopicPartitionOffset> topicPartitions) {
        stopConsumer();
        try {
            this.consumerThread.submit(() -> {
                Set<TopicPartition> assignment = this.consumer.assignment();
                if (topicPartitions.isEmpty()) {
                    this.consumer.seekToBeginning(assignment);
                    resetOffsetCache(assignment);
                } else {
                    Set<TopicPartition> assignedTopicPartition = assignment.stream().filter(match(topicPartitions)).collect(Collectors.toSet());
                    if (!assignedTopicPartition.isEmpty()) {
                        this.consumer.seekToBeginning(assignedTopicPartition);
                        resetOffsetCache(assignedTopicPartition);
                    }
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        startConsumer();
    }

    private void stopConsumer() {
        consumeTask.cancel(true);
    }

    private Predicate<TopicPartition> match(Collection<TopicPartitionOffset> topicPartitions) {
        return topicPartition -> topicPartitions.stream().filter(tp -> tp.getTopic() == null ||
                        (tp.getPartition() == null && tp.getTopic().equalsIgnoreCase(topicPartition.topic())) ||
                        (tp.getTopic().equalsIgnoreCase(topicPartition.topic()) && tp.getPartition().equals(topicPartition.partition())))
                .findAny().isPresent();
    }

    private void resetOffsetCache(Collection<TopicPartition> topicPartitions) {
        try {
            offsetManager.submit(() -> {
                if (topicPartitions.isEmpty()) {
                    this.offsetCache.keySet().forEach(this::initOffsetCache);
                } else {
                    topicPartitions.forEach(this::initOffsetCache);
                }
            }).get(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("reset offset error for topic partition {}", topicPartitions, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * if topic partitions is empty then seek all topic partitions assigned to this consumer to end
     */
    public void seekToEnd(Collection<TopicPartitionOffset> topicPartitions) {
        stopConsumer();
        try {
            this.consumerThread.submit(() -> {
                Set<TopicPartition> assignment = this.consumer.assignment();
                if (topicPartitions.isEmpty()) {
                    this.consumer.seekToEnd(topicPartitions);
                    resetOffsetCache(assignment);
                } else {
                    Set<TopicPartition> assignedTopicPartition = assignment.stream().filter(match(topicPartitions)).collect(Collectors.toSet());
                    if (!assignedTopicPartition.isEmpty()) {
                        this.consumer.seekToEnd(assignedTopicPartition);
                        resetOffsetCache(assignedTopicPartition);
                    }
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        startConsumer();
    }

    public void seekToOffset(Collection<TopicPartitionOffset> topicPartitions) {
        stopConsumer();
        try {
            this.consumerThread.submit(() -> {
                Set<TopicPartition> assignment = this.consumer.assignment();
                if (!topicPartitions.isEmpty()) {
                    Map<TopicPartition, Long> tpoMap = new HashMap<>(topicPartitions.size());
                    for (TopicPartitionOffset tpo : topicPartitions) {
                        TopicPartition topicPartition = new TopicPartition(tpo.getTopic(), tpo.getPartition());
                        if (assignment.contains(topicPartition)) {
                            tpoMap.put(topicPartition, tpo.getOffset());
                        }
                    }
                    if (!tpoMap.isEmpty()) {
                        for (Map.Entry<TopicPartition, Long> entry : tpoMap.entrySet()) {
                            this.consumer.seek(entry.getKey(), entry.getValue());
                        }
                        resetOffsetCache(tpoMap.keySet());
                    }
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        startConsumer();
    }

    public void seekToTimeStamp(Collection<TopicPartitionOffset> topicPartitions) {
        stopConsumer();
        try {
            this.consumerThread.submit(() -> {
                Set<TopicPartition> assignment = this.consumer.assignment();
                if (!topicPartitions.isEmpty()) {
                    Map<TopicPartition, Long> tptMap = new HashMap<>(topicPartitions.size());
                    for (TopicPartitionOffset tpo : topicPartitions) {
                        TopicPartition topicPartition = new TopicPartition(tpo.getTopic(), tpo.getPartition());
                        if (assignment.contains(topicPartition)) {
                            tptMap.put(topicPartition, tpo.getTimeStamp());
                        }
                    }
                    if (!tptMap.isEmpty()) {
                        Map<TopicPartition, OffsetAndTimestamp> map = this.consumer.offsetsForTimes(tptMap);
                        for (Map.Entry<TopicPartition, Long> entry : tptMap.entrySet()) {
                            this.consumer.seek(entry.getKey(), map.get(entry.getKey()).offset());
                        }
                        resetOffsetCache(tptMap.keySet());
                    }
                }
            }).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        startConsumer();
    }

}