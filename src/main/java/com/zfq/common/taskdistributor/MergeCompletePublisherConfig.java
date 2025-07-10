package com.zfq.common.taskdistributor;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@AutoConfigurationAfter({CellAutoConfiguration.class, CellBroadcastConfiguration.class})
public class MergeCompletePublisherConfig {

    @Bean("cellMergeCompleteTaskPublisher")
    @ConditionalOnMissingBean
    private MergeCompleteTaskPublisher mergeCompleteTaskPublisher(TaskService taskService) {
        return new MergeCompleteTaskPublisher(taskService);
    }
}
