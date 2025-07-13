package com.zfq.common.taskdistributor.merge;

public interface MergeFileEventBroadcaster<T> {

    void broadcastMergeEnd(T mergeFileInfo);

}
