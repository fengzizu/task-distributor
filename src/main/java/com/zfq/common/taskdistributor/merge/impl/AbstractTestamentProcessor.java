package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.merge.FileMerger;
import com.zfq.common.taskdistributor.merge.FileMergerTestament;
import com.zfq.common.taskdistributor.merge.FileMergerTestamentExecutor;
import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.task.ProcessorTask;
import com.zfq.common.taskdistributor.task.TaskOutput;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractTestamentProcessor implements Processor, FileMergerTestamentExecutor {

    private final FileMerger fileMerger;

    private final String group;

    public AbstractTestamentProcessor(FileMerger fileMerger, String group) {
        this.fileMerger = fileMerger;
        this.group = group;
    }

    @Override
    public void executeTestament(FileMergerTestament testament) {
        testament.workingDirectory().listFiles().forEach(mergeFile -> {
            fileMerger().writeAndDelete(mergeFile);
        });
        testament.workingDirectory().deleteIfExist();
    }

    @Override
    public FileMerger fileMerger() {
        return fileMerger;
    }

    @Override
    public TaskOutput process(ProcessorTask processorTask) {
        String text = processorTask.getMeta().get("testament");
        FileMergerTestament testament = deserialize(text);
        executeTestament(testament);
        return new TaskOutput();
    }

    @Override
    public String group() {
        return this.group;
    }

}
