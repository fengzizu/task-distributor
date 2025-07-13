package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.broadcast.MessageBroadcaster;
import com.zfq.common.taskdistributor.merge.MergeFileEventBroadcaster;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitEventBroadcaster implements MergeFileEventBroadcaster<String> {

    private final String topic;

    private final MessageBroadcaster messageBroadcaster;

    public CommitEventBroadcaster(String topic, MessageBroadcaster messageBroadcaster) {
        this.topic = topic;
        this.messageBroadcaster = messageBroadcaster;
    }

    @Override
    public void broadcastMergeEnd(String mergeKey) {
        messageBroadcaster.broadcastMessage(topic, mergeKey);
        log.info("commit broadcast to topic {} message {}", topic, mergeKey);
    }

}
