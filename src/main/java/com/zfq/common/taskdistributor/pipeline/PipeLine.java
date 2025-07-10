package com.zfq.common.taskdistributor.pipeline;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;

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
     * consumer is not thread safe
     * so need a single thread pool to use consumer
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
                    int processorCount, int queueSize, String containerFactoryName, ErrorHandler errorHandler, int retryTimes,
                    SuccessHandler successHandler) {
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


}
