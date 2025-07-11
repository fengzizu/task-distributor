package com.zfq.common.taskdistributor.task;

import com.zfq.common.taskdistributor.processor.Processor;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

public class Task implements Serializable {
    protected String taskName;
    protected Class<? extends Processor> taskProcessor;

    // used when task publisher and processor are not in the same code base
    protected String processorClassName;

    // default time is 1 day
    protected Duration expireTime = Duration.ofDays(1);

    // default delay is zero
    protected Duration delay = Duration.ZERO;
    protected Map<String, String> meta;

    //suggest only put necessary metadata to get pay load somewhere else, if payload data is samll the can put here
    protected Map<String, String> payloadMeta;

    // do not use this, this is only for redis to serialize and deserialize
    public Task() {
    }

    public Task(String taskName, Class<? extends Processor> taskProcessor, Duration expireTime, Map<String, String> meta, Map<String, String> payloadMeta) {
        this(taskName, taskProcessor, meta, payloadMeta);
        this.expireTime = expireTime;
    }

    public Task(String taskName, Class<? extends Processor> taskProcessor, Map<String, String> meta, Map<String, String> payloadMeta) {
        this.taskName = taskName;
        this.taskProcessor = taskProcessor;
        this.processorClassName = taskProcessor.getName();
        this.meta = meta;
        this.payloadMeta = payloadMeta;
    }

    public Task(String taskName, Class<? extends Processor> taskProcessor, Map<String, String> meta, Map<String, String> payloadMeta, Duration delay) {
        this(taskName, taskProcessor, meta, payloadMeta);
        this.delay = delay;
    }

    public Task(String taskName, Class<? extends Processor> taskProcessor, Duration expireTime, Map<String, String> meta, Map<String, String> payloadMeta, Duration delay) {
        this(taskName, taskProcessor, expireTime, meta, payloadMeta);
        this.delay = delay;
    }

    public Task(String taskName, String processorClassName, Duration expireTime, Map<String, String> meta, Map<String, String> payloadMeta) {
        this(taskName, processorClassName, meta, payloadMeta);
        this.expireTime = expireTime;
    }

    public Task(String taskName, String processorClassName, Map<String, String> meta, Map<String, String> payloadMeta) {
        this.taskName = taskName;
        this.processorClassName = processorClassName;
        this.meta = meta;
        this.payloadMeta = payloadMeta;
    }

    public Task(String taskName, String processorClassName, Map<String, String> meta, Map<String, String> payloadMeta, Duration delay) {
        this(taskName, processorClassName, meta, payloadMeta);
        this.delay = delay;
    }

    public Task(String taskName, String processorClassName, Duration expireTime, Map<String, String> meta, Map<String, String> payloadMeta, Duration delay) {
        this(taskName, processorClassName, expireTime, meta, payloadMeta);
        this.delay = delay;
    }

    public String getTaskName() {
        return taskName;
    }

    public Class<? extends Processor> getTaskProcessor() {
        return taskProcessor;
    }

    public Duration getExpireTime() {
        return expireTime;
    }

    public Map<String, String> getMeta() {
        return meta;
    }

    public Map<String, String> getPayloadMeta() {
        return payloadMeta;
    }

    public String getProcessorClassName() {
        return processorClassName;
    }

    public Duration getDelay() {
        return delay;
    }

    @Override
    public String toString() {
        return "Task{" +
                "taskName='" + taskName + '\'' +
                ", taskProcessor=" + taskProcessor +
                ", processorClassName= " + processorClassName + '\'' +
                ", expireTime=" + expireTime +
                ", meta=" + meta +
                '}';
    }

}
