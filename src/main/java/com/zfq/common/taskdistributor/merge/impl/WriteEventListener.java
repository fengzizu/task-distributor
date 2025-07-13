package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.broadcast.BroadcastListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.Topic;

import java.util.Arrays;
import java.util.List;

public class WriteEventListener extends BroadcastListener<MergeFileInfo> {
    private final FileMerger fileMerger;
    private final String topic;
    private final List<Topic> topicList;

    public WriteEventListener(FileMerger fileMerger, String topic) {
        this.fileMerger = fileMerger;
        this.topic = topic;
        this.topiclist = Arrays.asList(ChannelTopic.of(topic));
    }

    @Override
    public void onMessage(MergeFileInfo message, String topic) {
        fileMerger.removeAndwriteAndDelete(message);
    }

    @Override
    public List<Topic> getTopics() {
        return topicList;
    }

}