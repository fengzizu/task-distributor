package com.zfq.common.taskdistributor.merge.impl.disk.group;

import com.zfq.common.taskdistributor.merge.MergeFileInfo;
import com.zfq.common.taskdistributor.merge.MergeFileManager;
import com.zfq.common.taskdistributor.merge.impl.DefaultMergeFileWriter;
import com.zfq.common.taskdistributor.merge.impl.disk.DiskFileMerger;
import com.zfq.common.taskdistributor.merge.impl.disk.DiskMergeFile;
import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.task.TaskService;

import java.time.Duration;
import java.util.Set;

public class GroupDiskFileMerger extends DiskFileMerger {
    private final MergeFileManager manager;

    public GroupDiskFileMerger(DefaultMergeFileWriter writer, MergeFileManager manager, String workingPath, String name, TaskService taskService, Duration testamentDelay, int writePoolSize, String group) {
        super(writer, manager, workingPath, name, taskService, testamentDelay, writePoolSize, group);
        this.manager = manager;
    }

    @Override
    public DiskMergeFile createMergeFile(MergeFileInfo mergeFileInfo) {
        DiskMergeFile mergeFile = super.createMergeFile(mergeFileInfo);
        manager.register(mergeFile);
        return mergeFile;
    }

    @Override
    public void writeAndDelete(DiskMergeFile mergeFile) {
        super.writeAndDelete(mergeFile);
        manager.unregister(mergeFile.mergeFileInfo().mergeKey(), Set.of(mergeFile.getFile().getAbsolutePath()));
    }

    @Override
    public Class<? extends Processor> testamentProcoessorClass() {
        return GroupTestamentProcessor.class;
    }

    @Override
    public long period() {
        return 5;
    }

}
