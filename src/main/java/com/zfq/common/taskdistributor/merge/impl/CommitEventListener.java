package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.broadcast.BroadcastListener;
import com.zfq.common.taskdistributor.merge.FileMerger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.Topic;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class CommitEventListener extends BroadcastListener<String> {

    private final FileMerger fileMerger;

    private final List<Topic> topicList;

    public CommitEventListener(FileMerger fileMerger, String topic) {
        this.fileMerger = fileMerger;
        this.topicList = Arrays.asList(ChannelTopic.of(topic));
        log.info("commit event listening to topic {}", topic);
    }

    @Override
    public void onMessage(String mergeKey, String topic) {
        fileMerger.removeAndCommit(mergeKey);
    }

    @Override
    public List<Topic> getTopics() {
        return topicList;
    }

}
