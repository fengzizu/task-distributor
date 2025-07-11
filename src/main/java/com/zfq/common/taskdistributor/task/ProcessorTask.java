package com.zfq.common.taskdistributor.task;

import java.time.Duration;
import java.util.UUID;

public class ProcessorTask extends Task {

    public static final String KEY_SEPARATOR = "c_c";
    public static final String ROOT_KEY_PREFIX = "cell";
    private String key;
    private String parentKey;
    private Long createTime;

    public ProcessorTask() {
    }

    public ProcessorTask(Task task) {
        this(task, task.getDelay());
    }

    public ProcessorTask(Task task, String parentKey) {
        super(task.getTaskName(), task.getProcessorClassName(), task.getExpireTime(), task.getMeta(), task.getPayloadMeta(), task.getDelay());
        this.key = makeTaskKey(parentKey);
        this.parentKey = parentKey;
        this.createTime = System.currentTimeMillis();
    }

    public ProcessorTask(Task task, Duration delay) {
        super(task.getTaskName(), task.getProcessorClassName(), task.getExpireTime(), task.getMeta(), task.getPayloadMeta(), delay);
        this.key = makeTaskKey();
        this.createTime = System.currentTimeMillis();
    }

    private String makeTaskKey(String parentKey) {
        return parentKey + KEY_SEPARATOR + UUID.randomUUID();
    }

    private String makeTaskKey() {
        return ROOT_KEY_PREFIX + UUID.randomUUID();
    }

    public String getKey() {
        return key;
    }

    public String getParentKey() {
        return parentKey;
    }

    public Long getCreateTime() {
        return createTime;
    }

    @Override
    public String toString() {
        return "ProcessorTask[" +
                "key=" + key + '\'' +
                ", parentKey=" + parentKey + '\'' +
                ", taskName=" + taskName + '\'' +
                ", taskProcessor=" + taskProcessor +
                ", processorClassName=" + processorClassName + '\'' +
                ", meta=" + meta +
                ']';
    }
}