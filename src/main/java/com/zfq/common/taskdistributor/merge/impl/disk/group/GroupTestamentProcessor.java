package com.zfq.common.taskdistributor.merge.impl.disk.group;

import com.zfq.common.taskdistributor.task.TaskService;

public class GroupTestamentProcessor extends DiskTestamentProcessor {
    private final TaskService taskService;

    public GroupTestamentProcessor(FileMerger fileMerger, String group, TaskService taskService) {
        super(fileMerger, group);
        this.taskService = taskService;
    }

    @Override
    public void executeTestament(FileMergerTestament testament) {
        testament.workingDirectory().listFiles().forEach(mergeFile -> fileMerger().writeAndDelete(mergeFile));
        testament.workingDirectory().deleteIfExist();
    }

}
