package com.zfq.common.taskdistributor;


import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableAspectJAutoProxy
public class SwaggerCustomizeConfiguration {
    @Bean("cellDropdownListProcessor")
    @ConditionalOnMissingBean
    public DropDownListProcessor taskManager(){
        return new DropDownListProcessor();
    }
}
