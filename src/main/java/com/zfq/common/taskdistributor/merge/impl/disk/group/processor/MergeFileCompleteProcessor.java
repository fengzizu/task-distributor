package com.zfq.common.taskdistributor.merge.impl.disk.group.processor;

import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.task.ProcessorTask;
import com.zfq.common.taskdistributor.task.Task;
import com.zfq.common.taskdistributor.task.TaskOutput;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MergeFileCompleteProcessor implements Processor {
    public static final String MERGE_KEY = "mergeKey";
    private final CommitEventBroadcaster broadcaster;

    public MergeFileCompleteProcessor(CommitEventBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Override
    public TaskOutput process(ProcessorTask processorTask) {
        String mergeKey = processorTask.getMeta().get(MERGE_KEY);
        broadcaster.broadcastMergeEnd(mergeKey);
        Task commitCheck = new Task("commitCheck", MergeFileCommitCheckProcessor.class,
                Duration.ofDays(1), Map.of(MERGE_KEY, mergeKey), Collections.emptyMap(), Duration.ofSeconds(30));
        return new TaskOutput(List.of(commitCheck), "OK");
    }
}
