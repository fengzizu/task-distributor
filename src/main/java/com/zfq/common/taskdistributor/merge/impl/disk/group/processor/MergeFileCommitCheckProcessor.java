package com.zfq.common.taskdistributor.merge.impl.disk.group.processor;

import com.zfq.common.taskdistributor.merge.impl.disk.group.DiskMergeFileManager;
import com.zfq.common.taskdistributor.merge.impl.disk.group.RegisterInfo;
import com.zfq.common.taskdistributor.processor.Processor;
import com.zfq.common.taskdistributor.task.ProcessorTask;
import com.zfq.common.taskdistributor.task.Task;
import com.zfq.common.taskdistributor.task.TaskOutput;
import com.zfq.common.taskdistributor.task.TaskService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MergeFileCommitCheckProcessor implements Processor {
    public static final String FILE_NAME_SET = "fileNameSet";
    private final DiskMergeFileManager manager;
    private final TaskService taskService;

    public MergeFileCommitCheckProcessor(DiskMergeFileManager manager, TaskService taskService) {
        this.manager = manager;
        this.taskService = taskService;
    }

    @Override
    public TaskOutput process(ProcessorTask processorTask) {
        String mergeKey = processorTask.getMeta().get(MergeFileCompleteProcessor.MERGE_KEY);
        Map<String, RegisterInfo> registerFileMap = manager.registerInfo(mergeKey);
        Map<RegisterInfo, Set<String>> infoMap = transform(registerFileMap);
        taskService.startPublish(processorTask.getKey());

        for (Map.Entry<RegisterInfo, Set<String>> entry : infoMap.entrySet()) {
            RegisterInfo registerInfo = entry.getKey();
            Set<String> fileNameSet = entry.getValue();
            Duration delay = Duration.ZERO;
            Map<String, String> meta = Map.of(
                    FILE_NAME_SET,
                    fileNameSet.stream().collect(Collectors.joining(MergeFileWriteProcessor.DELIMITER))
            );

            Task task = new Task(
                    "writeMergeFile",
                    MergeFileWriteProcessor.class,
                    Duration.ofDays(1),
                    meta,
                    Collections.emptyMap(),
                    delay
            );

            taskService.publishSubTasks(processorTask.getKey(), List.of(task), registerInfo.getGroup());
            manager.unregister(mergeKey, fileNameSet);
        }

        taskService.endPublish(processorTask.getKey());
        return new TaskOutput();
    }
    private Map<RegisterInfo, Set<String>> transform(Map<String, RegisterInfo> registerFileMap) {
        Map<RegisterInfo, Set<String>> infoMap = new HashMap<>();
        for (Map.Entry<String, RegisterInfo> entry : registerFileMap.entrySet()) {
            String fileName = entry.getKey();
            RegisterInfo registerInfo = entry.getValue();
            /*
             * if one not committed in a group then the whole group will be set uncommitted
             */
            if (registerInfo.isCommitted()) {
                Set<String> set = infoMap.computeIfAbsent(registerInfo, info -> new HashSet<>());
                set.add(fileName);
            } else {
                log.warn("{} is not committed yet. will leave it to pod or testament to write", fileName);
            }
        }
        return infoMap;
    }
}
