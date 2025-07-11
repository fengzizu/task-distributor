package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.broadcast.MessageBroadcaster;
import com.zfq.common.taskdistributor.pipeline.PipeLine;
import com.zfq.common.taskdistributor.pipeline.PipeLineAdmin;
import com.zfq.common.taskdistributor.pipeline.PipeLineAdminController;
import lombok.extern.slf4j.Slf4j;
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
    public PipeLineAdmin pipeLineAdmin(List<PipeLine> pipeLinelist,
                                       @Value("${cell.pipeline.admin.broadcaster.topic:${spring.application.name}-pipeline-admin}") String broadcasterTopic,
                                       MessageBroadcaster messageBroadcaster) {
        return new PipeLineAdmin(pipeLinelist, broadcasterTopic, messageBroadcaster);
    }

    @Bean("cellPipeLineAdminController")
    @ConditionalOnMissingBean
    public PipeLineAdminController pipeLineAdminController(PipeLineAdmin pipeLineAdmin) {
        return new PipeLineAdminController(pipeLineAdmin);
    }

}