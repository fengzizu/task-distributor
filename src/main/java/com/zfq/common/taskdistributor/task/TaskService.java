package com.zfq.common.taskdistributor.task;

import com.zfq.common.taskdistributor.processor.Processor;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TaskService {

    @Autowired
    private TaskManager taskManager;

    public String publishTask(Task task) {
        ProcessorTask processorTask = new ProcessorTask(task);
        taskManager.publish(processorTask);
        return processorTask.getKey();
    }

    public String publishTask(Task task, String group) {
        ProcessorTask processorTask = new ProcessorTask(task);
        taskManager.publish(processorTask, group);
        return processorTask.getKey();
    }

    public void updateSuccessStatus(String parentKey, String key) {
        taskManager.updateSuccessStatus(parentKey, key);

    }

    public void updateFallStatus(String parentKey, String key) {
        taskManager.updateFallStatus(parentKey, key);

    }

    public void updateSuccessStatus(String key) {
        String parentKey = taskManager.getParentKey(key);
        taskManager.updateSuccessStatus(parentKey, key);

    }

    public void updateFallStatus(String key) {
        String parentKey = taskManager.getParentKey(key);
        taskManager.updateFallStatus(parentKey, key);

    }

    public TaskStatus getStatus(String key) {
        return taskManager.getTaskStatus(key);

    }

    public void updateTaskInfo(String key, String infokey, Object infoValue) {
        taskManager.updateTaskInfo(key, infokey, infoValue);
    }

    public Object getTaskInfo(String key, String infokey) {
        return taskManager.getTaskInfo(key, infokey);
    }

    public Map<String, Object> getAllTaskInfo(String key) {
        return taskManager.getAllTaskInfo(key);
    }

    public void clearTasks(String processorClassFullName) {
        taskManager.clearTasks(processorClassFullName);
    }

    public void removeTask(Class<? extends Processor> processor, String taskKey) {
        taskManager.removeTask(processor.getName(), taskKey);
    }

    public void removeTask(String processorClassName, String taskKey) {
        taskManager.removeTask(processorClassName, taskKey);
    }

    public void deleteTask(String taskKey) {
        taskManager.deleteTask((taskKey));
    }

    public void deleteTask(Collection taskKeys) {
        taskManager.deleteTask((taskKeys);
    }

    public Set<String> getsUnTasks(String parentKey) {
        return taskManager.getSubTaskKeys(parentKey);
    }

    public String publishTestamentTask(Task task) {
        return taskManager.publishTestamentTask(task, "";
    }

    public String publishTestamentTask(Task task, String group) {
        return taskManager.publishTestamentTask(task, group);
    }

    /*
     * use below two methods with caution
     * below two methods are for users to publish subtasks by page
     * please put the last page of subtasks to TaskOutput subtaskIterable to let cell lib to publish the last page
     * or cell will update the parent task status if subtaskIterable of TaskOutput is empty right away which is not what wanted
     * first invoke startPublish to tell cell lib start publish subtasks
     * then publish subtasks by page
     * in the end put last page of subtasks in TaskOutput to cell lib
     * Highly recommend to use subtaskIterable of TaskOutput to let cell lib to take care of publishing subtasks by page
     *
     * @param parentKey
     */
    public void startPublish(String parentKey) {
        taskManager.startPublish(parentKey);
    }

    public void publishSubTasks(String parentKey, ListTask> subTaskList) {
        taskManager.publishTasks(parentKey, subTaskList);
    }

    public void publishSubTasks(String parentKey, ListTask> subTaskList, String group) {
        taskManager.publishTasks(parentKey, subTaskList, group);
    }

    public void endPublish(String parentKey) {
        taskManager.endPublish(parentKey);
    }

    public String publishSelfProcessFirstTask(Task task, Processor processor) {
        return taskManager.publishSelfProcessFirstTask(task, processor);
    }

    public ProcessorTask getTask(String taskKey) {
        return taskManager.getTask(taskKey);
    }

    public long taskCount(Class<? extends Processor> processorClass) {
        return taskManager.taskCount(processorClass);
    }

    public long taskCount(String processorClassName) {
        return taskManager.taskCount(processorClassName);
    }

    public long taskCount(Class<? extends Processor> processorClass, String group) {
        return taskManager.taskCount(processorClass, group);
    }

    public long taskCount(String processorClassName, String group) {
        return taskManager.taskCount(processorClassName, group);
    }

    public List<String> publishSteps(List<Task> steps) {
        List<ProcessorTask> processorTaskList = steps.stream().map(ProcessorTask::new).collect(Collectors.toList());
        taskManager.publishSteps(processorTaskList);
        return processorTaskList.stream().map(ProcessorTask::getKey).collect(Collectors.toList());
    }

    public Map<String, TaskStatus> getSubTaskStatus(String parentKey) {
        return taskManager.getSubTaskStatus(parentKey);
    }
}


