package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.broadcast.MessageBroadcaster;
import com.zfq.common.taskdistributor.task.TaskService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

import java.io.File;
import java.time.Duration;
import java.util.UUID;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@AutoConfigureAfter({CellAutoConfiguration.class, CellBroadcastConfiguration.class, MergeCompletePublisherConfig.class})
public class RegisterFileMergeConfig {
    @Value("${cell.merge.file.testament.delay.seconds:300}")
    private int testamentDelaySeconds = 300;
    @Value("${cell.merge.file.working.directory:tmp}")
    private String workingDirectory = "tmp";
    @Value("${cell.merge.file.write.pool.size:1}")
    private int writePoolSize = 1;
    @Value("${cell.merge.file.commit.event.topic:${spring.application.name}-cell-file-merge-commit-event-topic}")
    private String fileMergeCommitEventTopic = "fileMergeCommitEventDefaultTopic";
    @Value("${cell.merge.file.testament.group:${OSE_CLUSTER:group}}")
    private String group = "";
    @Value("${cell.merge.file.register.key.suffix:-cell-file-merge-register-key-suffix}")
    private String registerkeySuffix = "-cell-file-merge-register-key-suffix";

    @Bean("cellGroupDiskFileMerger")
    @ConditionalOnMissingBean
    private GroupDiskFileMerger diskFileMerger(TaskService taskService, DefaultMergeFileWriter writer, DiskMergeFileManager reporter) {
        String name = UUID.randomUUID().toString();
        return new GroupDiskFileMerger(writer, reporter,
                workingDirectory + File.separator + name, name, taskService, Duration.ofSeconds(testamentDelaySeconds), writePoolSize, group);

    }

    @Bean(" cellGroupDiskTestamentProcessor")
    @ConditionalOnMissingBean
    private GroupTestamentProcessor diskTestamentProcessor(GroupDiskFileMerger fileMerger, TaskService taskService) {
        return new GroupTestamentProcessor(fileMerger, group, taskService);
    }

    @Bean("cellCommitEventListener")
    @ConditionalOnMissingBean
    private CommitEventListener mergeFileEndEventListener(GroupDiskFileMerger fileMerger) {
        return new CommitEventlistener(fileMerger, fileMergeCommitEventTopic);
    }

    @Bean(" cellCommitEventBroadcaster")
    @ConditionalOnMissingBean
    private CommitEventBroadcaster mergeFileEventBroadcaster(MessageBroadcaster messageBroadcaster) {
        return new CommitEventBroadcaster(fileMergeCommitEventTopic, messageBroadcaster);
    }

    @Bean(" cellMergeFileCompleteProcessor")
    @ConditionalOnMissingBean
    private MergeFileCompleteProcessor mergeFileCompleteProcessor(CommitEventBroadcaster broadcaster) {
        return new MergeFileCompleteProcessor(broadcaster);
    }

    @Bean(" cellMergeF ileCommitCheckProcessor")
    @ConditionalOnMissingBean
    private MergeFileCommitCheckProcessor mergeFileCommitCheckProcessor(DiskMergeFileManager fileManager, TaskService taskService) {
        return new MergeFileCommitCheckProcessor(fileManager, taskService);
    }

    @Bean(" cellMergeFileWriteProcessor")
    @ConditionalOnMissingBean
    private MergeFileWriteProcessor mergeFileWriteProcessor(DefaultMergeFileWriter writer) {
        return new MergeFileWriteProcessor(group, writer);
    }

}
