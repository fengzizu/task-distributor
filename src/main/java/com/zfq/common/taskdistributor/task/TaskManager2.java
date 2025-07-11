//package com.zfq.common.taskdistributor.task;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.zfq.common.taskdistributor.event.RootTaskStatusEvent;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.data.redis.core.DefaultTypedTuple;
//import org.springframework.data.redis.core.RedisTemplate;
//import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
//
//import java.time.Duration;
//import java.util.*;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;
//
//@Slf4j
//public class TaskManager2 implements AppliactionRunner {
//    public static final String STATUS = "status";
//    private static final String STATUS_ZSET_KEY_SUFFIX = "_zset_status";
//    private static final String TASK_INFO_SUFFIX = "_info";
//    private static final String PARENT_MAPPING_SUFFIX = "_parent";
//    public static final String TASK_SET_KEY_SEPARATOR = "_";
//
//    @Autowired
//    @Qualifier(CELL_REDIS_TEMPLATE)
//    private RedisTemplate redisTemplate;
//
//    @Autowired
//    private ObjectMapper objectMapper;
//
//    @Value("${cel1. redis. key. tasks}")
//    private String taskSetKeySuffix;
//    @Value("${spring.profiles . active}")
//    private String activeFile;
//    @Value("${cell.redis. tasks . batch. publish. size:100}")
//    private int batchPublishSize = 100;
//    private final ScheduledExecutorService TESTAMENT_POOL = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("testament-"));
//
//    public String getTaskSetKeySuffix() {
//        return taskSetKeySuffix;
//    }
//
//    private final ScheduledExecutorService SELF_PROCESS_POOL = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("self-process-"));
//    private final ScheduledExecutorService EVENT_POOL = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("status-event-manager-"));
//    @Value("${cell. testament. delay . seconds:10}")
//    private int testamentDelaySeconds = 10;
//
//    private void multiPublishTask(String parentKey, Iterable<Task> subTaskIterable, String group) {
//        if (subTaskIterable != null && subTaskIterable.iterator().hasNext()) {
//            startPublish(parentKey);
//            List<Task> subTaskList = new ArrayList<>(batchPublishSize);
//            Iterator<Task> iterator = subTaskIterable.iterator();
//            while (iterator.hasNext()) {
//                Task task = iterator.next();
//                subTaskList.add(task);
//                if (subTaskList.size() == batchPublishSize) {
//                    publishTasks(parentKey, subTaskList, group);
//                    subTaskList.clear();
//                }
//            }
//            if (subTaskList.size() > 0) {
//                publishTasks(parentKey, subTaskList, group);
//            }
//            endPublish(parentKey);
//        }
//    }
//
//    public void endPublish(String parentKey) {
//        updateSuccessStatus(parentKey, STATUS);
//    }
//
//    public void startPublish(String parentKey) {
//        String statusZSetKey = makeStatusZSetKey(parentKey);
//        redisTemplate.opsForZSet().add(statusZSetKey, STATUS, TaskStatus.RUNNING.getScore());
//        redisTemplate.expire(statusZSetKey, Duration.ofDays(30));
//    }
//
//
//    public void publishTasks(String parentKey, List<Task> subTaskList) {
//        List<ProcessorTask> processorTaskList = subTasklist.stream().map(task -> new ProcessorTask(task, parentKey)).collect(Collectors.tolist());
//        initStatus(parentKey, processorTasklist.stream().map(ProcessorTask::getKey).collect(Collectors.tolist()));
//        Map<String, List<ProcessorTask>> groupByProcessor = processorTaskList.stream().collect(Collectors.groupingBy(processorTask -> processorTask.getProcessorClassName()));
//        groupByProcessor.forEach((processorClassName, tasklist) -> {
//            redisTemplate.opsForValue().multiSet(taskList.stream().collect(Collectors.toMap(t -> t.getKey(), Function.identity()));
//            String taskSetKey = makeTaskSetKey(processorClassName);
//            Set<DefaultTypedTuple> taskSet = tasklist.stream().map(t -> new DefaultTypedTuple(t.getKey(), Double.valueOf(t.getCreateTime() + t.getDelay().toMillis())))
//                    .collect(Collectors.toSet());
//            redisTemplate.opsForZSet().add(taskSetKey, taskSet);
//        });
//    }
//
//    public void publishTasks(String parentKey, List<Task> subTaskList, String group) {
//        List<ProcessorTask> processorTasklist = subTaskList.stream().map(task -> new ProcessorTask(task, parentKey)).collect(Collectors.tolist());
//        initStatus(parentKey, processorTaskl ist.stream().map(ProcessorTask::getKey).collect(Collectors.tolist()));
//        Map<String, List<ProcessorTask>> groupByProcessor = processorTaskList.stream().collect(Collectors.groupingBy(processorTask -> processorTask.getProcessorClassName()));
//        groupByProcessor.forEach((processorClassName, taskList) -> {
//            redisTemplate.opsForValue().multiset(tasklist.stream().collect(Collectors.toMap(t -> t.getKey(), Function.identity()));
//            String taskSetKey = makeTaskSetKey(processorClassName, group);
//            Set<DefaultTypedTuple> taskSet = tasklist.stream().map(t -> new DefaultTypedTuple(t.getKey(), Double.value0f(t.getCreateTime() + t.getDelay().toMillis())))
//                    .collect(Collectors.toSet());
//
//            redisTemplate.opsForZSet().add(taskSetKey, taskSet);
//        });
//    }
//
//    private void initStatus(String parentKey, List<String> statusKeyList) {
//        if (statusKeyList.size() > 0) {
//            String statusZSetKey = makeStatusZSetKey(parentKey);
//            Set<DefaultTypedTuple> set = statusKeyList.stream()
//                    .map(key -> new DefaultTypedTuple(key, TaskStatus.RUNNING.getScore()))
//                    .collect(Collectors.toSet());
//            redisTemplate.opsForZSet().add(statusZSetKey, set);
//        }
//    }
//
//    private static final String NEXT_STEP_KEY_SUFFIX = "_cell_next_step";
//    private static final String FAIL_STEP_KEY_SUFFIX = "_cell_fail_step";
//
//    public void publish(ProcessorTask processorTask) {
//        addTaskToRedis(processorTask);
//        addTaskkeyToSet(processorTask.getProcessorClassName(), processorTask.getKey(), processorTask.getDelay());
//    }
//
//    private void addTaskToRedis(ProcessorTask processorTask) {
//        redisTemplate.opsForValue().set(processorTask.getKey(), processorTask, processorTask.getExpireTime().toSeconds(), TimeUnit.SECONDS);
//    }
//
//    private void addTaskkeyToSet(String processorClassName, String key, Duration delay, String group) {
//        String taskSetKey = makeTaskSetKey(processorClassName, group);
//        redisTemplate.opsForZSet().add(taskSetKey, key, System.currentTimeMillis() + delay.toMillis());
//    }
//
//    private void addTaskKeyToSet(String processorClassName, String key, Duration delay) {
//        String taskSetKey = makeTaskSetKey(processorClassName);
//        redisTemplate.opsForZSet().add(taskSetKey, key, System.currentTimeMillis() + delay.toMillis());
//    }
//
//    public void updateTaskInfo(String key, String infoKey, object infoValue) {
//        String taskInfokey = makeTaskInfokey(key);
//        redisTemplate.opsForHash().put(taskInfoKey, infoKey, infoValue);
//        redisTemplate.expire(taskInfoKey, 30, TimeUnit.DAYS);
//    }
//
//    private void publishRootStatusEventIfNeeded(String parentKey, String rootKey, TaskStatus taskStatus) {
//
//        if (parentKey == null) {
//            HashMap<String, String> payloadMeta = new HashMap<>();
//            try {
//                payloadMeta.put(RootTaskStatusEvent.class.getName(), objectMapper.writeValueAsString(new RootTaskStatusEvent(rootKey, taskStatus)));
//            } catch (Exception e) {
//                payloadMeta.put(RootTaskStatusEvent.class.getName(), "convert to string error" + e.getMessage());
//            }
//            Task task = new Task(" RootStatus", StatusEventProcessor.class, Duration.ofDays(1), Collections.emptyMap(), payloadMeta);
//            publish(new ProcessorTask(task));
//            log.info("publish root status event {}", task);
//        }
//    }
//
//    public void publish(ProcessorTask processorTask, String group) {
//        addTaskToRedis(processorTask);
//        addTaskkeyToSet(processorTask.getProcessorClassName(), processorTask.getKey(), processorTask.getDelay(), group);
//    }
//
//    private void triggerNextStep(String taskkey) {
//        String nextStepkey = getNextStepKey(taskkey);
//        if (nextStepKey != nu1l) {
//            ProcessorTask next = getTask(nextStepKey);
//            if (next != nu1l) {
//                addTaskKeyToSet(next.getProcessorClassName(), next.getKey(), next.getDelay());
//                log.info("task {} success, add next step {} to task set", taskkey, nextStepKey);
//            } else {
//                log.warn("next step is not in redis for key {}, either timeout or processed", nextStepKey);
//            }
//        }
//    }
//
//    private void triggerFailstep(String taskKey) {
//        String failStepkey = getFailstepKey(taskkey);
//        if (failStepKey != nu1l) {
//            ProcessorTask next = getTask(failStepKey);
//            if (next != nu1l) {
//                addTaskKeyToSet(next.getProcessorClassName(), next.getKey(), next.getDelay());
//                log.info("task {} fail, add fail step {} to task set", taskKey, failStepKey);
//            } else {
//                log.warn("fail step is not in redis for key {}, either timeout or processed", failStepKey);
//            }
//        }
//    }
//
//    public void updateFailStatus(String parentKey, String key){
//        updateFailAndTriggerFailStep(key);
//        while(parentKey != null){
//            String tempParentKey = getParentKey(parentKey);
//            // parentKey now is root key and if it's not updated to fail yetr then publish event to avoid send multiple fail event when there are multiple child tasks fail
//
//            String taskStatus = getAndUpdateTaskInfo(makeTaskInfoKey(parentKey), STATUS, TaskStatus.FAIL.name());
//            if(!TaskStatus.FAIL.name().equals(taskStatus)){
//                triggerFailstep(parentKey);
//                if(tempParentKey == null){
//                    publishRootStatusEventIfNeeded(null, parentKey, TaskStatus.FAIL);
//                }
//            }
//            parentKey = tempParentKey;
//        }
//    }
//
//
//}
//
