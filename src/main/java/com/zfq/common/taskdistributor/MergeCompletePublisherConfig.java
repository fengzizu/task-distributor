package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.task.TaskService;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@AutoConfigureAfter({CellAutoConfiguration.class, CellBroadcastConfiguration.class})
public class MergeCompletePublisherConfig {

    @Bean("cellMergeCompleteTaskPublisher")
    @ConditionalOnMissingBean
    private MergeCompleteTaskPublisher mergeCompleteTaskPublisher(TaskService taskService) {
        return new MergeCompleteTaskPublisher(taskService);
    }
}
