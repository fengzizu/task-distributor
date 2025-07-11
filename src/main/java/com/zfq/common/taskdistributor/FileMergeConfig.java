package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.broadcast.MessageBroadcaster;
import com.zfq.common.taskdistributor.task.TaskService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.File;
import java.time.Duration;
import java.util.UUID;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@AutoConfigureAfter({CellAutoConfiguration.class, CellBroadcastConfiguration.class, RegisterFileMergeConfig.class})
public class FileMergeConfig {

    @Value("${cell.merge . file . testament . delay . seconds :300}")
    private int testamentDelaySeconds = 300;
    @Value("${cell.merge . file . working . directory:tmp}")
    private String workingDirectory = "tmp";
    @Value("${cell.merge.file .write.pool.size:1}")
    private int writePoolSize = 1;
    @Value("${cell.merge.file.event.topic:${spring. application. name}-cell-file-merge-event-topic}")
    private String fileMergeEventTopic = "fileMergeEventDefaultTopic";
    @Value("${cell.merge.file.testament.group:${spring . profiles . active}}")
    private String group = " ";
    @Value("${cell.merge.file.register.key.suffix:-cell-file-merge-register-key-suffix}")
    private String registerKeySuffix = "-cell-file-merge-register-key-suffix";

    @Bean("cellDiskMergeFileWriter")
    @ConditionalOnMissingBean
    private DefaultMergeFileWriter diskMergeFileWriter(ApplicationContext context) {
        return new DefaultMergeFileWriter(context);
    }

    @Bean("cellDiskMergeFileManager")
    @ConditionalOnMissingBean
    private DiskMergeFileManager diskMergeFileReporter(@Qualifier(CELLREDIS_ TEMPLATE) RedisTemplate redisTemplate) {
        return new DiskMergeFileManager(redisTemplate, registerKeySuffix, group);
    }

    @Bean("cel1DiskFileMerger")
    @ConditionalOnMissingBean
    private DiskFileMerger diskFileMerger(TaskService taskService, DefaultMergeFileWriter writer, DiskMergeFileManager manager) {
        String name = UUID.randomUUID().toString();
        return new DiskFileMerger(writer, manager,
                workingDirectory + File.separator + name, name, taskService, Duration.ofSeconds(testamentDelaySeconds), writePoolSize, group);
    }

    @Bean("cellDiskTestamentProcessor")
    @ConditionalOnMissingBean
    private DiskTestamentProcessor diskTestamentProcessor(DiskFileMerger fileMerger) {
        return new DiskTestamentProcessor(fileMerger, group);
    }

    @Bean(" cellwriteEventL istener")
    @ConditionalOnMissingBean
    private WriteEventListener mergeFileEndEventListener(DiskFileMerger fileMerger) {
        return new WriteEventListener(fileMerger, fileMergeEventTopic);
    }

    @Bean("cellWriteEventBroadcaster")
    @ConditionalOnMissingBean
    private WriteEventBroadcaster mergeFileEventBroadcaster(MessageBroadcaster messageBroadcaster) {
        return new WriteEventBroadcaster(fileMergeEventTopic, messageBroadcaster);
    }


}
