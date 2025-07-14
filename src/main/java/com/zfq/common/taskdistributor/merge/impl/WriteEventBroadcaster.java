package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.broadcast.MessageBroadcaster;
import com.zfq.common.taskdistributor.merge.MergeFileEventBroadcaster;
import com.zfq.common.taskdistributor.merge.MergeFileInfo;

public class WriteEventBroadcaster implements MergeFileEventBroadcaster<MergeFileInfo> {

    private final String topic;
    private final MessageBroadcaster messageBroadcaster;

    public WriteEventBroadcaster(String topic, MessageBroadcaster messageBroadcaster) {
        this.topic = topic;
        this.messageBroadcaster = messageBroadcaster;
    }

    @Override
    public void broadcastMergeEnd(MergeFileInfo mergeFileInfo) {
        messageBroadcaster.broadcastMessage(topic, mergeFileInfo);
    }

}
