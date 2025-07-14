package com.zfq.common.taskdistributor.merge.impl.disk.group.processor;

import com.zfq.common.taskdistributor.merge.MergeFile;
import com.zfq.common.taskdistributor.merge.impl.DefaultMergeFileWriter;
import com.zfq.common.taskdistributor.merge.impl.disk.DiskMergeFile;
import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.task.ProcessorTask;
import com.zfq.common.taskdistributor.task.TaskOutput;
import com.zfq.common.taskdistributor.task.TaskStatus;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;


@Slf4j
public class MergeFileWriteProcessor implements Processor {
    public static final String DELIMITER = ",";
    public static final String NEED_CLEAN_FOLDER = "NEED_CLEAN_FOlDER";
    private final String group;
    private final DefaultMergeFileWriter writer;

    public MergeFileWriteProcessor(String group, DefaultMergeFileWriter writer){
        this.group = group;
        this.writer = writer;
    }
    @Override
    public TaskOutput process(ProcessorTask processorTask) {
        String[] fileNames = processorTask.getMeta().get(MergeFileCommitCheckProcessor.FILE_NAME_SET).split(DELIMITER);
        Optional<MergeFile> sample = toMergeFileStream(fileNames).findFirst();
        sample.ifPresent(s -> {
            Stream<MergeFile> diskMergeFileStream = toMergeFileStream(fileNames);
            writer.writeFiles(s, diskMergeFileStream);
            Stream.of(fileNames).map(File::new).forEach(File::delete);
        });
        if (String.valueOf(true).equalsIgnoreCase(processorTask.getMeta().get(NEED_CLEAN_FOLDER))) {
            File parentFile = new File(fileNames[0]).getParentFile();
            if (parentFile.exists() && parentFile.listFiles().length == 0) {
                parentFile.delete();
            }
        }
        return new TaskOutput(TaskStatus.SUCCESS);
    }

    private Stream<MergeFile> toMergeFileStream(String[] fileNames) {
        return Stream.of(fileNames).map(fileName -> {
            try {
                return (MergeFile) new DiskMergeFile<>(fileName);
            } catch (Exception e) {
                log.error("no file for {}", fileName);
                return null;
            }
        }).filter(Objects::nonNull);
    }

    @Override
    public String group() {
        return this.group;
    }

}
