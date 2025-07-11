package com.zfq.common.taskdistributor.processor;

import com.zfq.common.taskdistributor.task.ProcessorTask;
import com.zfq.common.taskdistributor.task.TaskManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ProcessorManager implements InitializingBean, ApplicationContextAware, SmartLifecycle, ApplicationRunner {

    public static final long LOCK_TIMEOUT = Duration.ofSeconds(300).toMillis();
    public static final int EXTEND_PERIOD_SECOND = 10;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private TaskManager taskManager;

    @Value("${cell_processor.number:1}")
    private int workerNumber = 1;

    private ApplicationContext context;

    private Map<Class<? extends Processor>, Processor> processorMap;

    private AtomicInteger sharedAvailableWorkerCount;

    private ScheduledExecutorService dispatchPool;

    private ScheduledExecutorService sharedLockPool;

    private ExecutorService sharedWorkerPool;

    private volatile boolean isRunning = false;

    private List<ExecutorService> exclusiveWorkerPoolList = new ArrayList<>();

    private List<ExecutorService> exclusiveLockPoolList = new ArrayList<>();

    @Override
    public void afterPropertiesSet() {
        isRunning = true;
        sharedAvailableWorkerCount = new AtomicInteger(workerNumber);
        Map<String, Processor> beansOfType = this.context.getBeansOfType(Processor.class);
        beansOfType.values().forEach(processor -> {
            /**
             * in case it's an anonymous class
             */
            Class<? extends Processor> aClass = (Class<? extends Processor>) AopUtils.getTargetClass(processor);
            if (aClass.isAnonymousClass()) {
                processorMap.put((Class<? extends Processor>) aClass.getSuperclass(), processor);
            } else {
                processorMap.put(aClass, processor);
            }
        });
        sharedWorkerPool = Executors.newFixedThreadPool(workerNumber, new CustomizableThreadFactory("worker-"));
        dispatchPool = Executors.newScheduledThreadPool(processorMap.size(), new CustomizableThreadFactory("dispatcher-"));
        sharedLockPool = Executors.newScheduledThreadPool(workerNumber, new CustomizableThreadFactory("locker-"));
    }

    private Set<String> requestTaskKeys(String taskSetKey, int count) {
        try {
            DefaultRedisScript script = new DefaultRedisScript<>("local list = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[3]) " +
                    "if (list ~= nil and #list > 0) " +
                    "then for i=1, #list do redis.call('ZADD', KEYS[1], ARGV[2], list[i]) end return list " +
                    "else return nil " +
                    "end");
            script.setResultType(Set.class);
            List<String> taskKeyList = (List<String>) redisTemplate.execute(script, Arrays.asList(taskSetKey), System.currentTimeMillis(), System.currentTimeMillis() + EXTEND_PERIOD_SECOND * 1000, count);
            return Optional.ofNullable(taskKeyList)
                    .map(list -> (Set) new HashSet<>(list))
                    .orElse(Collections.emptySet());
        } catch (Throwable e) {
            log.error("request task error from key {} to key {}", taskSetKey, e);
            return Collections.emptySet();
        }
    }

    private void startProcessors() {
        for (Class<? extends Processor> processorClass : processorMap.keySet()) {
            startProcessor(processorClass);
        }
    }

    private void startProcessor(Class<? extends Processor> processorClass) {
        String processorName = processorClass.getName();
        Processor processor = processorMap.get(processorClass);
        String taskSetKey = taskManager.makeTaskSetKey(processorName, processor.group());
        log.info("register processor {} listening to {}", processorName, taskSetKey);
        dispatchPool.execute(new DispatchTask(taskSetKey, processor));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        log.info("Graceful shutdown start");
        isRunning = false;
        sharedAvailableWorkerCount.set(Integer.MIN_VALUE);
        dispatchPool.shutdown();
        sharedWorkerPool.shutdown();
        exclusiveWorkerPoolList.forEach(ExecutorService::shutdown);
        sharedLockPool.shutdown();
        exclusiveLockPoolList.forEach(ExecutorService::shutdown);
        log.info("Graceful shutdown complete");
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void run(ApplicationArguments args) {
        startProcessors();
    }

    class DispatchTask implements Runnable {
        private String taskSetKey;
        private Processor processor;

        private AtomicInteger availableWorkerCount;

        private ExecutorService workerPool;

        private ScheduledExecutorService lockPool;

        public DispatchTask(String taskSetKey, Processor processor) {
            this.taskSetKey = taskSetKey;
            this.processor = processor;
            int workerCount = processor.workerCount();
            if (workerCount <= 0) {
                availableWorkerCount = sharedAvailableWorkerCount;
                workerPool = sharedWorkerPool;
                lockPool = sharedLockPool;
            } else {
                availableWorkerCount = new AtomicInteger(workerCount);
                workerPool = Executors.newFixedThreadPool(workerCount, new CustomizableThreadFactory("exclusive-worker-"));
                lockPool = Executors.newScheduledThreadPool(workerCount, new CustomizableThreadFactory("exclusive-locker-"));
                exclusiveWorkerPoolList.add(workerPool);
                exclusiveLockPoolList.add(lockPool);
            }
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    int availableWorker = availableWorkerCount.get();
                    if (availableWorker > 0) {
                        Set<String> taskKeySet = requestTaskKeys(taskSetKey, availableWorker);
                        if (taskKeySet.size() == 0) {
                            Thread.sleep(1000);
                        } else {
                            for (String taskKey : taskKeySet) {
                                log.debug("dispatch task key {}", taskKey);
                                processTask(taskKey);
                            }
                        }
                    }
                } catch (Throwable e) {
                    log.error("dispatch error for processor {}", processor.getClass().getName(), e);
                }
            }
            log.info("Graceful shutdown for processor {}", processor.getClass().getName());
        }

        private void processTask(String taskKey) {
            if (taskKey != null) {
                ProcessorTask processorTask = taskManager.getTask(taskKey);
                if (processorTask != null) {
                    ScheduledFuture<?> lockFuture = lockPool.scheduleAtFixedRate(() -> lockTask(taskKey), 0, EXTEND_PERIOD_SECOND / 5, TimeUnit.SECONDS);
                    availableWorkerCount.decrementAndGet();
                    workerPool.execute(() -> {
                        try {
                            taskManager.processTask(processorTask, processor);
                        } finally {
                            availableWorkerCount.incrementAndGet();
                            lockFuture.cancel(true);
                            log.debug("done lock task {}", taskKey);
                            redisTemplate.opsForZSet().remove(taskSetKey, taskKey);
                            redisTemplate.delete(taskKey);
                        }
                    });
                } else {
                    log.info("remove taskKey {} as there is no task for the key", taskKey);
                    redisTemplate.opsForZSet().remove(taskSetKey, taskKey);
                }
            }
        }

        private boolean lockTask(String taskKey) {
            try {
                log.debug("lock task key {}", taskKey);
                return redisTemplate.opsForZSet().add(taskSetKey, taskKey, System.currentTimeMillis() + EXTEND_PERIOD_SECOND * 1000);
            } catch (Throwable e) {
                log.error("lock task key {} error", taskKey, e);
                return false;
            }
        }
    }

}
