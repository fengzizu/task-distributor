package com.zfq.common.taskdistributor.task;

import com.zfq.common.taskdistributor.event.RootTaskStatusEvent;
import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.processor.StatusEventProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;
import static com.zfq.common.taskdistributor.task.ProcessorTask.KEY_SEPARATOR;
import static com.zfq.common.taskdistributor.task.ProcessorTask.ROOT_KEY_PREFIX;

@Slf4j
public class TaskManager implements ApplicationRunner {

    public static final String STATUS = "status";
    private static final String STATUS_ZSET_KEY_SUFFIX = "_zset_status";
    private static final String TASK_INFO_SUFFIX = "_info";
    private static final String PARENT_MAPPING_SUFFIX = "_parent";
    public static final String TASK_SET_KEY_SEPARATOR = "}";

    @Autowired
    @Qualifier(CELL_REDIS_TEMPLATE)
    private RedisTemplate redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;
    @Value("${cell.redis.key.tasks}")
    private String taskSetKeySuffix;

    @Value("${spring.profiles.active}")
    private String activeFile;

    @Value("${cell.redis.tasks.batch.publish.size}")
    private int batchPublishSize = 100;

    private final ScheduledExecutorService TESTAMENT_POOL = Executors.newScheduledThreadPool(2, new CustomizableThreadFactory("testament-"));

    public String getTaskSetKeySuffix() {
        return taskSetKeySuffix;
    }

    private final ScheduledExecutorService SELF_PROCESS_POOL = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("self-process-"));
    private final ScheduledExecutorService EVENT_POOL = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("status-event-manager-"));
    @Value("${cell.testament.delay.seconds:10}")
    private int testamentDelaySeconds = 10;

    private void multiPublishTask(String parentKey, Iterable<Task> subTaskIterable, String group) {
        if (subTaskIterable != null && subTaskIterable.iterator().hasNext()) {
            startPublish(parentKey);
            List<Task> subTaskList = new ArrayList<>(batchPublishSize);
            Iterator<Task> iterator = subTaskIterable.iterator();
            while (iterator.hasNext()) {
                Task task = iterator.next();
                subTaskList.add(task);
                if (subTaskList.size() == batchPublishSize) {
                    publishTasks(parentKey, subTaskList, group);
                    subTaskList.clear();
                }
            }
            if (subTaskList.size() > 0) {
                publishTasks(parentKey, subTaskList, group);
            }
            endPublish(parentKey);
        }
    }

    public void endPublish(String parentKey) {
        updateSuccessStatus(parentKey, STATUS);
    }

    public void startPublish(String parentKey) {
        String statusZSetKey = makeStatusZSetKey(parentKey);
        redisTemplate.opsForZSet().add(statusZSetKey, STATUS, TaskStatus.RUNNING.getScore());
        redisTemplate.expire(statusZSetKey, Duration.ofDays(30));
    }

    public void publishTasks(String parentKey, List<Task> subTaskList) {
        List<ProcessorTask> processorTaskList = subTaskList.stream().map(task -> new ProcessorTask(task, parentKey)).collect(Collectors.toList());
        initStatus(parentKey, processorTaskList.stream().map(ProcessorTask::getKey).collect(Collectors.toList()));
        Map<String, List<ProcessorTask>> groupByProcessor = processorTaskList.stream().collect(Collectors.groupingBy(processorTask -> processorTask.getProcessorClassName()));
        groupByProcessor.forEach((processorClassName, taskList) -> {
            redisTemplate.opsForValue().multiSet(taskList.stream().collect(Collectors.toMap(t -> t.getKey(), Function.identity())));
            String taskSetKey = makeTaskSetKey(processorClassName);
            Set<DefaultTypedTuple> taskSet = taskList.stream().map(t -> new DefaultTypedTuple(t.getKey(), Double.valueOf(t.getCreateTime() + t.getDelay().toMillis())))
                    .collect(Collectors.toSet());
            redisTemplate.opsForZSet().add(taskSetKey, taskSet);
        });
    }

    public void publishTasks(String parentKey, List<Task> subTaskList, String group) {
        List<ProcessorTask> processorTaskList = subTaskList.stream().map(task -> new ProcessorTask(task, parentKey)).collect(Collectors.toList());
        initStatus(parentKey, processorTaskList.stream().map(ProcessorTask::getKey).collect(Collectors.toList()));
        Map<String, List<ProcessorTask>> groupByProcessor = processorTaskList.stream().collect(Collectors.groupingBy(processorTask -> processorTask.getProcessorClassName()));
        groupByProcessor.forEach((processorClassName, taskList) -> {
            redisTemplate.opsForValue().multiSet(taskList.stream().collect(Collectors.toMap(t -> t.getKey(), Function.identity())));
            String taskSetKey = makeTaskSetKey(processorClassName, group);
            Set<DefaultTypedTuple> taskSet = taskList.stream().map(t -> new DefaultTypedTuple(t.getKey(), Double.valueOf(t.getCreateTime() + t.getDelay().toMillis())))
                    .collect(Collectors.toSet());
            redisTemplate.opsForZSet().add(taskSetKey, taskSet);
        });
    }

    private void initStatus(String parentKey, List<String> statusKeyList) {
        if (statusKeyList.size() > 0) {
            String statusZSetKey = makeStatusZSetKey(parentKey);
            Set<DefaultTypedTuple> set = statusKeyList.stream()
                    .map(key -> new DefaultTypedTuple(key, TaskStatus.RUNNING.getScore()))
                    .collect(Collectors.toSet());
            redisTemplate.opsForZSet().add(statusZSetKey, set);
        }
    }

    private static final String NEXT_STEP_KEY_SUFFIX = "_next";
    private static final String FAIL_STEP_KEY_SUFFIX = "_fail";

    public void publish(ProcessorTask processorTask) {
        addTaskToRedis(processorTask);
        addTaskKeyToSet(processorTask.getProcessorClassName(), processorTask.getKey(), processorTask.getDelay());
    }

    private void addTaskToRedis(ProcessorTask processorTask) {
        redisTemplate.opsForValue().set(processorTask.getKey(), processorTask, processorTask.getExpireTime().toSeconds(), TimeUnit.SECONDS);
    }

    private void addTaskKeyToSet(String processorClassName, String key, Duration delay, String group) {
        String taskSetKey = makeTaskSetKey(processorClassName, group);
        redisTemplate.opsForZSet().add(taskSetKey, key, System.currentTimeMillis() + delay.toMillis());
    }

    private void addTaskKeyToSet(String processorClassName, String key, Duration delay) {
        String taskSetKey = makeTaskSetKey(processorClassName);
        redisTemplate.opsForZSet().add(taskSetKey, key, System.currentTimeMillis() + delay.toMillis());
    }

    public void updateTaskInfo(String key, String infoKey, Object infoValue) {
        String taskInfoKey = makeTaskInfoKey(key);
        redisTemplate.opsForHash().put(taskInfoKey, infoKey, infoValue);
        redisTemplate.expire(taskInfoKey, 30, TimeUnit.DAYS);
    }

    private void publishRootStatusEventIfNeeded(String parentKey, String rootKey, TaskStatus taskStatus) {
        if (parentKey == null) {
            HashMap<String, String> payloadMeta = new HashMap<>();
            try {
                payloadMeta.put(RootTaskStatusEvent.class.getName(), objectMapper.writeValueAsString(new RootTaskStatusEvent(rootKey, taskStatus)));
            } catch (Exception e) {
                payloadMeta.put(RootTaskStatusEvent.class.getName(), "convert to string error" + e.getMessage());
            }
            Task task = new Task("RootStatus", StatusEventProcessor.class, Duration.ofDays(1), Collections.emptyMap(), payloadMeta);
            publish(new ProcessorTask(task));
            log.info("publish root status event {}", task);
        }
    }

    public void publish(ProcessorTask processorTask, String group) {
        addTaskToRedis(processorTask);
        addTaskKeyToSet(processorTask.getProcessorClassName(), processorTask.getKey(), processorTask.getDelay(), group);
    }

    private void triggerNextStep(String taskKey) {
        String nextStepKey = getNextStepKey(taskKey);
        if (nextStepKey != null) {
            ProcessorTask next = getTask(nextStepKey);
            if (next != null) {
                addTaskKeyToSet(next.getProcessorClassName(), next.getKey(), next.getDelay());
                log.info("task {} success, add next step {} to task set", taskKey, nextStepKey);
            } else {
                log.warn("next step is not in redis for key {}, either timeout or processed", nextStepKey);
            }
        }
    }

    private void triggerFailStep(String taskKey) {
        String failStepKey = getFailStepKey(taskKey);
        if (failStepKey != null) {
            ProcessorTask next = getTask(failStepKey);
            if (next != null) {
                addTaskKeyToSet(next.getProcessorClassName(), next.getKey(), next.getDelay());
                log.info("task {} fail, add fail step {} to task set", taskKey, failStepKey);
            } else {
                log.warn("fail step is not in redis for key {}, either timeout or processed", failStepKey);
            }
        }
    }

    public void updateFailStatus(String parentKey, String key) {
        updateFailAndTriggerFailStep(key);
        while (parentKey != null) {
            String tempParentKey = getParentKey(parentKey);
            String taskStatus = getAndUpdateTaskInfo(makeTaskInfoKey(parentKey), STATUS, TaskStatus.FAIL.name());
            if (TaskStatus.FAIL.name().equals(taskStatus)) {
                triggerFailStep(parentKey);
                if (tempParentKey == null) {
                    publishRootStatusEventIfNeeded(null, parentKey, TaskStatus.FAIL);
                }
            }
            parentKey = tempParentKey;
        }
    }

    private void updateSuccessAndTriggerNextStep(String key) {
        updateTaskInfo(key, STATUS, TaskStatus.SUCCESS.name());
        triggerNextStep(key);
    }

    private String getAndUpdateTaskInfo(String taskInfoKey, String key, String value) {
        DefaultRedisScript script = new DefaultRedisScript<>(
                "local v = redis.call('HGET', KEYS[1], ARGV[1]) " +
                        "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) " +
                        "return v");
        script.setResultType(String.class);
        return (String) this.redisTemplate.execute(script, (RedisSerializer) null, this.redisTemplate.getValueSerializer(), Arrays.asList(taskInfoKey), new Object[]{this.redisTemplate.getKeySerializer().serialize(key), this.redisTemplate.getValueSerializer().serialize(value)});
    }

    public boolean updateSuccessAndGetParentStatus(String statusZSetKey, String key) {
        DefaultRedisScript script = new DefaultRedisScript(
                "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]) " +
                        "local count = redis.call('ZCARD', KEYS[1]) " +
                        "local successCount = redis.call('ZCOUNT', KEYS[1], 2.0001, 10000000.00) " +
                        "if(count == successCount) then return true else return false end");
        script.setResultType(Boolean.class);
        return (boolean) redisTemplate.execute(script, Arrays.asList(statusZSetKey), key, TaskStatus.SUCCESS.getScore());
    }

    private void updateFailAndTriggerFailStep(String key) {
        updateTaskInfo(key, STATUS, TaskStatus.FAIL.name());
        triggerFailStep(key);
    }

    public String getParentKey(String key) {
        if (key.startsWith(ROOT_KEY_PREFIX)) {
            if (key.contains(KEY_SEPARATOR)) {
                return key.split(KEY_SEPARATOR)[0];
            } else {
                return null;
            }
        } else {
            return (String) redisTemplate.opsForValue().getAndExpire(makeParentMappingKey(key), Duration.ofDays(30));
        }
    }

    public String makeStatusZSetKey(String key) {
        return key + STATUS_ZSET_KEY_SUFFIX;
    }

    private String makeParentMappingKey(String key) {
        return key + PARENT_MAPPING_SUFFIX;
    }

    private String makeTaskInfoKey(String key) {
        return key + TASK_INFO_SUFFIX;
    }

    public TaskStatus getTaskStatus(String key) {
        String status = (String) redisTemplate.opsForHash().get(makeTaskInfoKey(key), STATUS);
        return Optional.ofNullable(status).map(s -> TaskStatus.valueOf(s)).orElse(TaskStatus.RUNNING);
    }

    public Object getTaskInfo(String key, String infoKey) {
        return redisTemplate.opsForHash().get(makeTaskInfoKey(key), infoKey);
    }

    public Map<String, Object> getAllTaskInfo(String key) {
        return redisTemplate.opsForHash().entries(makeTaskInfoKey(key));
    }

    public void clearTasks(String processorClassFullName) {
        String taskSetKey = makeTaskSetKey(processorClassFullName);
        redisTemplate.delete(taskSetKey);
    }

    public void removeTask(String processorClassFullName, String taskKey) {
        String taskSetKey = makeTaskSetKey(processorClassFullName);
        redisTemplate.opsForZSet().remove(taskSetKey, taskKey);
    }

    private void removeTimeoutStatusEvent() {
        String taskSetKey = makeTaskSetKey(StatusEventProcessor.class.getName());
        redisTemplate.opsForZSet().removeRangeByScore(taskSetKey, 0, System.currentTimeMillis());
        redisTemplate.opsForZSet().removeRange(taskSetKey, 1000, Long.MAX_VALUE);
    }

    public String makeTaskSetKey(String processorClassName) {
        return "{" + processorClassName + TASK_SET_KEY_SEPARATOR + taskSetKeySuffix + resolveMachine() + "}";
    }

    public String makeTaskSetKey(String processorClassName, String group) {
        return "{" + processorClassName + TASK_SET_KEY_SEPARATOR + taskSetKeySuffix + resolveMachine() + group + "}";
    }

    public String resolveMachine() {
        return RunningMachineUtils.resolveMachine(activeFile);
    }

    public void processTask(ProcessorTask processorTask, Processor processor) {
        String key = processorTask.getKey();
        try {
            if (key.length() > 40000) {
                throw new RuntimeException("task level too deep " + key.split(KEY_SEPARATOR).length);
            }
            TaskOutput output = processor.process(processorTask);
            if (output.getFailStep() != null) {
                ProcessorTask failStep = new ProcessorTask(output.getFailStep());
                addTaskToRedis(failStep);
                setFailStepLink(processorTask.getKey(), failStep.getKey());
            }
            if (TaskStatus.SUCCESS.equals(output.getStatus())) {
                Iterable<StatusNode> statusNodeIterable = output.getStatusNodeIterable();
                if (output.getSubTaskIterable().iterator().hasNext() || statusNodeIterable.iterator().hasNext()) {
                    multiPublishStatusNode(key, statusNodeIterable);
                    multiPublishTask(key, output.getSubTaskIterable(), processor.group());
                } else {
                    updateSuccessStatus(processorTask.getParentKey(), key);
                }
            } else if (TaskStatus.FAIL.equals(output.getStatus())) {
                updateFailStatus(processorTask.getParentKey(), key);
            }
        } catch (Throwable e) {
            log.error("process task error {}.", processorTask, e);
            try {
                updateFailStatus(processorTask.getParentKey(), key);
                processor.onError(processorTask, e);
            } catch (Throwable throwable) {
                log.error("on error exception task {}.", processorTask, e);
            }
        } finally {
            log.info("processed task {}", processorTask);
        }
    }

    private void setFailStepLink(String currentTaskKey, String failStepKey) {
        redisTemplate.opsForValue().set(makeFailStepKey(currentTaskKey), failStepKey, Duration.ofDays(7));
    }

    private void multiPublishStatusNode(String parentKey, Iterable<StatusNode> statusNodeIterable) {
        if (statusNodeIterable != null && statusNodeIterable.iterator().hasNext()) {
            List<String> statusKeyList = StreamSupport.stream(statusNodeIterable.spliterator(), false).map(StatusNode::getKey).collect(Collectors.toList());
            startPublish(parentKey);
            initStatus(parentKey, statusKeyList);
            Map<String, String> mapping = statusKeyList.stream().map(this::makeParentMappingKey).collect(Collectors.toMap(Function.identity(), mappingKey -> parentKey));
            redisTemplate.opsForValue().multiSet(mapping);
            endPublish(parentKey);
        }
    }

    public ProcessorTask getTask(String taskKey) {
        try {
            return (ProcessorTask) redisTemplate.opsForValue().get(taskKey);
        } catch (Exception e) {
            log.error("error get task using key {}", taskKey, e);
            return null;
        }
    }

    public Set<String> getSubTaskKeys(String parentKey) {
        String key = makeStatusZSetKey(parentKey);
        return (Set<String>) redisTemplate.opsForZSet().range(key, 0, -1).stream()
                .filter(tk -> !STATUS.equalsIgnoreCase((String) tk)).collect(Collectors.toSet());
    }

    public Map<String, TaskStatus> getSubTaskStatus(String parentKey) {
        String key = makeStatusZSetKey(parentKey);
        Set<ZSetOperations.TypedTuple> set = redisTemplate.opsForZSet().rangeWithScores(key, 0, -1);
        return set.stream().filter(tp -> !STATUS.equalsIgnoreCase((String) tp.getValue()))
                .collect(Collectors.toMap(tp -> (String) tp.getValue(), tp -> TaskStatus.fromScore(tp.getScore())));
    }

    public void deleteTask(String taskKey) {
        redisTemplate.delete(taskKey);
    }

    public void deleteTask(Collection taskKeys) {
        redisTemplate.delete(taskKeys);
    }

    public String publishTestamentTask(Task task, String group) {
        Duration delay = task.getDelay();
        if (task.getDelay().isZero()) {
            delay = Duration.ofSeconds(testamentDelaySeconds);
        }
        ProcessorTask processorTask = new ProcessorTask(task, delay);
        String key = processorTask.getKey();
        publish(processorTask, group);
        Duration interval = delay.dividedBy(5);
        TESTAMENT_POOL.scheduleAtFixedRate(() -> {
            try {
                addTaskKeyToSet(processorTask.getProcessorClassName(), processorTask.getKey(), processorTask.getDelay(), group);
                log.debug("still alive extend testament time {}", processorTask);
            } catch (Throwable e) {
                log.error("extend testament task key {} error", processorTask.getKey(), e);
            }
        }, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        return key;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        EVENT_POOL.scheduleAtFixedRate(() -> {
            try {
                removeTimeoutStatusEvent();
            } catch (Throwable e) {
                log.error("remove timeout status event error", e);
            }
        }, 2, 10, TimeUnit.MINUTES);
    }

    public String publishSelfProcessFirstTask(Task task, Processor processor) {
        Duration delay = Duration.ofSeconds(10);
        ProcessorTask processorTask = new ProcessorTask(task, delay);
        publish(processorTask);
        String key = processorTask.getKey();
        Duration interval = delay.dividedBy(5);
        ScheduledFuture<?> scheduledFuture = SELF_PROCESS_POOL.scheduleAtFixedRate(() -> addTaskKeyToSet(processor.getClass().getName(), key, processorTask.getDelay()), 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        try {
            processTask(processorTask, processor);
            String taskSetKey = makeTaskSetKey(processor.getClass().getName());
            redisTemplate.opsForZSet().remove(taskSetKey, key);
            redisTemplate.delete(key);
        } finally {
            scheduledFuture.cancel(true);
        }
        return key;
    }

    public long taskCount(Class<? extends Processor> processorClass) {
        String taskSetKey = makeTaskSetKey(processorClass.getName());
        Long size = redisTemplate.opsForZSet().size(taskSetKey);
        return size;
    }

    public long taskCount(String processorClassName) {
        String taskSetKey = makeTaskSetKey(processorClassName);
        Long size = redisTemplate.opsForZSet().size(taskSetKey);
        return size;
    }

    public long taskCount(Class<? extends Processor> processorClass, String group) {
        String taskSetKey = makeTaskSetKey(processorClass.getName(), group);
        Long size = redisTemplate.opsForZSet().size(taskSetKey);
        return size;
    }

    public long taskCount(String processorClassName, String group) {
        String taskSetKey = makeTaskSetKey(processorClassName, group);
        Long size = redisTemplate.opsForZSet().size(taskSetKey);
        return size;
    }

    public void updateSuccessStatus(String parentKey, String key) {
        updateSuccessAndTriggerNextStep(key);
        while (parentKey != null) {
            String statusZSetKey = makeStatusZSetKey(parentKey);
            boolean isParentSuccess = updateSuccessAndGetParentStatus(statusZSetKey, key);
            if (isParentSuccess) {
                updateSuccessAndTriggerNextStep(parentKey);
                key = parentKey;
                parentKey = getParentKey(parentKey);
                publishRootStatusEventIfNeeded(parentKey, key, TaskStatus.SUCCESS);
            } else {
                parentKey = null;
            }
        }
    }

    public void publishSteps(List<ProcessorTask> steps) {
        ProcessorTask processorTask = steps.get(0);
        String previousKey = processorTask.getKey();
        for (int i = 1; i < steps.size(); i++) {
            ProcessorTask current = steps.get(i);
            addTaskToRedis(current);
            setStepLink(previousKey, current.getKey());
            previousKey = current.getKey();
        }
        publish(processorTask);
    }

    private void setStepLink(String previousStepKey, String nextStepKey) {
        redisTemplate.opsForValue().set(makeNextStepKey(previousStepKey), nextStepKey, Duration.ofDays(7));
    }

    private String makeNextStepKey(String key) {
        return key + NEXT_STEP_KEY_SUFFIX;
    }

    private String makeFailStepKey(String key) {
        return key + FAIL_STEP_KEY_SUFFIX;
    }

    private String getNextStepKey(String key) {
        return (String) redisTemplate.opsForValue().get(makeNextStepKey(key));
    }

    private String getFailStepKey(String key) {
        return (String) redisTemplate.opsForValue().get(makeFailStepKey(key));
    }
}