package com.zfq.common.taskdistributor;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
public class CellPublisherConfiguration {

    @Bean("cellTaskManager")
    @ConditionalOnMissingBean
    public TaskManager taskManager() {
        return new TaskManager();
    }

    @Bean("cellITaskService")
    @ConditionalOnMissingBean
    public TaskService taskService() {
        return new TaskService();
    }

}
