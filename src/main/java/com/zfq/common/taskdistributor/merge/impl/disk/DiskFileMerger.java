package com.zfq.common.taskdistributor.merge.impl.disk;

import com.zfq.common.taskdistributor.merge.FileMergerTestament;
import com.zfq.common.taskdistributor.merge.MergeFileInfo;
import com.zfq.common.taskdistributor.merge.MergeFileManager;
import com.zfq.common.taskdistributor.merge.impl.AbstractFileMerger;
import com.zfq.common.taskdistributor.merge.impl.DefaultMergeFileWriter;
import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.task.Task;
import com.zfq.common.taskdistributor.task.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

@Slf4j
public class DiskFileMerger extends AbstractFileMerger<DiskMergeFile, DefaultMergeFileWriter, MergeFileManager, DiskDirectory> implements ApplicationRunner {

    protected final String workingPath;

    protected final TaskService taskService;

    protected final Duration testamentDelay;

    protected final String name;

    protected final String group;

    public DiskFileMerger(DefaultMergeFileWriter writer, MergeFileManager committer,
                          String workingPath, String name, TaskService taskService, Duration testamentDelay, int writePoolSize, String group) {
        super(writer, committer, new DiskDirectory(new File(workingPath)), writePoolSize);
        this.taskService = taskService;
        this.workingPath = workingPath;
        this.name = name;
        this.group = group;
        this.testamentDelay = testamentDelay;
        this.workingDirectory().createIfNotExist();
    }

    private String createAbsolutePath(MergeFileInfo mergeFileInfo) {
        return workingPath
                + File.separator + mergeFileInfo.mergeKey() + "_" + name + "_" + System.currentTimeMillis();
    }

    @Override
    public DiskMergeFile createMergeFile(MergeFileInfo mergeFileInfo) {
        try {
            String absolutePath = createAbsolutePath(mergeFileInfo);
            File file = new File(absolutePath);
            try {
                file.createNewFile();
            } catch (IOException e) {
                log.error("working path is not existed {} ", workingPath);
                this.workingDirectory.createIfNotExist();
                file.createNewFile();
            }
            DiskMergeFile diskMergeFile = new DiskMergeFile(file, mergeFileInfo);
            diskMergeFile.append(SerializationUtils.serialize(mergeFileInfo));
            return diskMergeFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public FileMergerTestament createTestament() {
        return () -> workingDirectory;
    }

    @Override
    public void publishTestament(FileMergerTestament testament) {
        HashMap<String, String> meta = new HashMap<>();
        meta.put("testament", testament.workingDirectory().serialize());
        Task task = new Task(this.getClass().getName() + "testament", testamentProcoessorClass(), Duration.ofSeconds(Integer.MAX_VALUE), meta,
                Collections.emptyMap(), testamentDelay);
        taskService.publishTestamentTask(task, this.group);
        log.info("published testament {}", task);
    }

    public Class<? extends Processor> testamentProcoessorClass() {
        return DiskTestamentProcessor.class;
    }

    @Override
    public void run(ApplicationArguments args) {
        publishTestament(createTestament());
    }

    public String getWorkingPath() {
        return workingPath;
    }

    public TaskService getTaskService() {
        return taskService;
    }

    public Duration getTestamentDelay() {
        return testamentDelay;
    }

    public String getName() {
        return name;
    }


}