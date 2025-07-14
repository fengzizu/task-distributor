package com.zfq.common.taskdistributor.pipeline;

import com.zfq.common.taskdistributor.task.RunningMachineUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class RedisErrorHandlerPipeLine extends PipeLine {

    private final RedisTemplate redisTemplate;

    private final ScheduledExecutorService redisErrorMessageManager;

    private final long partitionCacheSize;

    @Value("${spring.profiles.active}")
    private String activeFile;

    private Consumer errorConsumer;

    private Duration messageTimeout;

    public RedisErrorHandlerPipeLine(Collection<String> inputTopics, String groupId, MessageProcessor messageProcessor,
                                     int processorCount, int queueSize, String containerFactoryName, ErrorHandler errorHandler, int retryTimes, SuccessHandler successHandler, RedisTemplate redisTemplate,
                                     long partitionCacheSize, Duration messageTimeout) {
        super(inputTopics, groupId, messageProcessor, processorCount, queueSize, containerFactoryName, errorHandler, retryTimes, successHandler);
        this.redisTemplate = redisTemplate;
        this.partitionCacheSize = partitionCacheSize;
        redisErrorMessageManager = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory("redis-error-message"));
        this.messageTimeout = messageTimeout;
    }

    @Override
    protected void onError(ConsumerRecord consumerRecord, Throwable throwable) {
        addDelay(consumerRecord);
        log.info("put message from topic {} partition {} offset {} to redis for retry", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), throwable);
    }

    private String makeKey(String topic, int partition) {
        return topic + "_" + partition + "_" + RunningMachineUtils.resolveMachine(activeFile) + "_offset_cell";
    }

    @Override
    public void run(ApplicationArguments args) {
        super.run(args);
        AbstractKafkaListenerContainerFactory factory = (AbstractKafkaListenerContainerFactory) applicationContext.getBean(this.containerFactoryName);
        ConsumerFactory consumerFactory = factory.getConsumerFactory();
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        this.errorConsumer = consumerFactory.createConsumer(groupId + "_error_retry", null, null, props);
        redisErrorMessageManager.scheduleAtFixedRate(() -> retryErrorMessage(), 1, 2, TimeUnit.SECONDS);
    }

    private void retryErrorMessage() {
        try {
            for (TopicPartition topicPartition : getTopicPartitions()) {
                String offsetKey = makeKey(topicPartition.topic(), topicPartition.partition());
                Long size = redisTemplate.opsForZSet().size(offsetKey);

                if (size <= partitionCacheSize) {
                    Set<Long> offsets = redisTemplate.opsForZSet().rangeByScore(offsetKey, 0, System.currentTimeMillis());
                    for (Long offset : offsets) {
                        try {
                            processOffsetMessage(topicPartition, offset, offsetKey, false);
                        } catch (Throwable e) {
                            log.error("retry message from redis error. topic {} partition {} offset {} ", topicPartition.topic(), topicPartition.partition(), offset, e);
                            redisTemplate.opsForZSet().add(offsetKey, offset, System.currentTimeMillis() + Duration.ofMinutes(10).toMillis());
                        }
                    }
                } else {
                    for (int i = 0; i < size - partitionCacheSize; i++) {
                        ZSetOperations.TypedTuple<Long> typedTuple = redisTemplate.opsForZSet().popMin(offsetKey);
                        try {
                            processOffsetMessage(topicPartition, typedTuple.getValue(), offsetKey, true);
                        } catch (Throwable e) {
                            log.error("last tried error for topic {} partition {} offset {}", topicPartition.topic(), topicPartition.partition(), typedTuple.getValue(), e);
                        }
                    }
                }
            }
        } catch (Throwable e) {
            log.error("retry message from redis error", e);
        }
    }

    private Set<TopicPartition> getTopicPartitions() {
        try {
            return (Set<TopicPartition>) CompletableFuture.supplyAsync(() -> offsetCache.keySet().stream().collect(Collectors.toSet()), offsetManager)
                    .get(1, TimeUnit.MINUTES);
        } catch (Throwable e) {
            log.error("error get topic partitions.", e);
            return Collections.emptySet();
        }
    }

    private void processOffsetMessage(TopicPartition topicPartition, Long offset, String offsetKey, boolean isLast) {
        ConsumerRecord consumerRecord = fetchConsumeRecord(topicPartition, offset);
        if (consumerRecord != null && consumerRecord.offset() == offset) {
            try {
                CompletableFuture future = messageProcessor.process(consumerRecord);
                future.whenCompleteAsync((r, e) -> Optional.ofNullable(e)
                        .ifPresentOrElse(ex -> addDelay(consumerRecord),
                                () -> {
                                    successHandler.handleSuccess(consumerRecord, r);
                                    redisTemplate.opsForZSet().remove(offsetKey, offset);
                                    log.info("retry message from redis success. topic {} partition {} offset {} ", topicPartition.topic(), topicPartition.partition(), offset);
                                }
                        )).orTimeout(10, TimeUnit.MINUTES);
            } catch (Throwable e) {
                if (isLast) {
                    log.warn("last tried error for topic {} partition {} offset {} handle to errorHandler", topicPartition.topic(), topicPartition.partition(), offset, e);
                    errorHandler.handleError(consumerRecord, e);
                } else if (System.currentTimeMillis() - consumerRecord.timestamp() <= messageTimeout.toMillis()) {
                    log.warn("message is expired topic {} partition {} offset {} ", topicPartition.topic(), topicPartition.partition(), offset, e);
                    redisTemplate.opsForZSet().remove(offsetKey, offset);
                    errorHandler.handleError(consumerRecord, new RuntimeException("message expired", e));
                } else {
                    throw e;
                }
            }
        } else {
            redisTemplate.opsForZSet().remove(offsetKey, offset);
            log.warn("message is not in kafka topic {} partition {} offset {} ", topicPartition.topic(), topicPartition.partition(), offset);
        }
    }

    private void addDelay(ConsumerRecord consumerRecord) {
        long currentTimeMillis = System.currentTimeMillis();
        long messageTime = consumerRecord.timestamp();
        long timePassed = currentTimeMillis - messageTime;
        double score = currentTimeMillis + Math.min(timePassed, Duration.ofMinutes(10).toMillis());
        redisTemplate.opsForZSet().add(makeKey(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), score);
    }

    private ConsumerRecord fetchConsumeRecord(TopicPartition topicPartition, Long offset) {
        errorConsumer.assign(Collections.singletonList(topicPartition));
        errorConsumer.seek(topicPartition, offset);
        ConsumerRecords records = errorConsumer.poll(pollTimeout);
        if (records.count() == 1) {
            return (ConsumerRecord) records.iterator().next();
        }
        return null;
    }

    @Override
    public void stop() {
        super.stop();
        redisErrorMessageManager.execute(() -> errorConsumer.close());
        redisErrorMessageManager.shutdown();
    }

}
