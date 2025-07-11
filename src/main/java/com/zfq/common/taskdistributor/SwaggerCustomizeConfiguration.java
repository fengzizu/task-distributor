package com.zfq.common.taskdistributor;


import com.zfq.common.taskdistributor.swagger.DropDownListProcessor;
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
