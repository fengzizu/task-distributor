package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.event.CellStatusEventListener;
import com.zfq.common.taskdistributor.processor.StatusEventProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
public class CellStatusListenerConfiguration {

    @Bean("cellStatusEventProcessor")
    @ConditionalOnMissingBean
    @ConditionalOnBean(CellStatusEventListener.class)
    public StatusEventProcessor statusEventProcessor() {
        return new StatusEventProcessor();
    }

}
