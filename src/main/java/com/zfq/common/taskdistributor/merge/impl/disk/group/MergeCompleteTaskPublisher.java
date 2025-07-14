package com.zfq.common.taskdistributor.merge.impl.disk.group;

import com.zfq.common.taskdistributor.task.Task;
import com.zfq.common.taskdistributor.task.TaskService;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

public class MergeCompleteTaskPublisher {
    private final TaskService taskService;

    public MergeCompleteTaskPublisher(TaskService taskService) {
        this.taskService = taskService;
    }

    public String publishMergeCompleteTask(String mergeKey) {
        Task task = new Task("mergeComplete", MergeFileCompleteProcessor.class,
                Duration.ofDays(1), Map.of(MergeFileCompleteProcessor.MERGE_KEY, mergeKey), Collections.emptyMap());
        return taskService.publishTask(task);
    }

}
