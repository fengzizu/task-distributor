package com.zfq.common.taskdistributor.pipeline;

import java.util.Collections;
import java.util.Set;

public class SeekEvent {

    private SeekToType seekToType;
    private Set<TopicPartitionOffset> seekToSet = Collections.emptySet();

    public SeekEvent() {
    }

    public SeekEvent(SeekToType seekToType) {
        this.seekToType = seekToType;
    }

    public SeekEvent(SeekToType seekToType, Set<TopicPartitionOffset> seekToSet) {
        this.seekToType = seekToType;
        this.seekToSet = seekToSet;
    }

    public Set<TopicPartitionOffset> getSeekToSet() {
        return seekToSet;
    }

    public SeekToType getSeekToType() {
        return seekToType;
    }

}

