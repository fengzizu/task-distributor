package com.zfq.common.taskdistributor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@AutoConfigureAfter(CellStatusListenerConfiguration.class)
public class CellAutoConfiguration {

    @Bean("cellTaskManager")
    @ConditionalOnMissingBean
    public TaskManager taskManager() {
        return new TaskManager();
    }

    @Bean("cellTaskService")
    @ConditionalOnMissingBean
    public TaskService taskService() {
        return new TaskService();
    }

    @Bean("cellRedisLockInterceptor")
    @ConditionalOnMissingBean
    public RedisLockInterceptor redisLockInterceptor() {
        return new RedisLockInterceptor();
    }

    @Bean("cellObjectMapper")
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean("cellProcessorManager")
    @ConditionalOnMissingBean
    public ProcessorManager processorManager() {
        return new ProcessorManager();
    }

}
