package com.zfq.common.taskdistributor;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Pipeline;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

import java.util.List;

@Slf4j
@EnableCellBroadcast
@EnableAspectJAutoProxy
@PropertySource("cell.yml")
public class PipeLineAdminConfiguration {

    @Bean("cellPipeLineAdmin")
    @ConditionalOnMissingBean
    public PipeLineAdmin pipeLineAdmin(List<Pipeline> pipeLinelist,
                                       @Value("${cel1.pipeline.admin.broadcaster.topic:${spring.application.name}-pipeline - admin}") String broadcasterTopic,
                                       MessageBroadcaster messageBroadcaster) {
        return new PipeLineAdmin(pipelineList, broadcasterTopic, messageBroadcaster);
    }

    @Bean("cellPipeLineAdminController")
    @ConditionalOnMissingBean
    public PipeLineAdminController pipeLineAdminController(PipeLineAdmin pipeLineAdmin) {
        return new PipeLineAdminController(pipeLineAdmin);
    }
}