package com.zfq.common.taskdistributor.task;


import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Collections;

@Getter
@Generated
@AllArgsConstructor
@NoArgsConstructor
public class TaskOutput {

    private TaskStatus status;
    private Iterable<Task> subTaskIterable = Collections.emptyList();
    private Iterable<StatusNode> statusNodeIterable = Collections.emptyList();
    private String message = "please provide some message for task";
    private Task failStep;

    public TaskOutput(TaskStatus status) {
        this.status = status;
    }

    public TaskOutput(TaskStatus status, Iterable<Task> subTaskIterable, String message) {
        this.status = status;
        this.subTaskIterable = subTaskIterable;
        this.message = message;
    }

    public TaskOutput(TaskStatus status, String message, Iterable<StatusNode> statusNodeIterable) {
        this.status = status;
        this.statusNodeIterable = statusNodeIterable;
        this.message = message;
    }

    public TaskOutput(Iterable<Task> subTaskIterable, String message) {
        this.status = TaskStatus.SUCCESS;
        this.subTaskIterable = subTaskIterable;
        this.message = message;
    }

    public TaskOutput(String message, Iterable<StatusNode> statusNodeIterable) {
        this.status = TaskStatus.SUCCESS;
        this.statusNodeIterable = statusNodeIterable;
        this.message = message;
    }


}
