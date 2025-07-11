package com.zfq.common.taskdistributor.pipeline;

public class TopicPartitionOffset {

    private String topic;

    private Integer partition;

    private Long offset;

    private Long timeStamp;

    public TopicPartitionOffset(String topic, Integer partition, Long offset, Long timeStamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timeStamp = timeStamp;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public Long getOffset() {
        return offset;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

}
