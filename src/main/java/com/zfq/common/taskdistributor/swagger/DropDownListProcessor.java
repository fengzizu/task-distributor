package com.zfq.common.taskdistributor.swagger;

import io.swagger.v3.oas.models.parameters.Parameter;
import org.springdoc.core.customizers.ParameterCustomizer;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodParameter;

import java.util.List;
import java.util.Optional;

public class DropDownListProcessor implements ParameterCustomizer, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public Parameter customize(Parameter parameterModel,
                               MethodParameter methodParameter) {
        DropDownList dropDownList = methodParameter.getParameterAnnotation(DropDownList.class);
        if (dropDownList != null) {
            Class<? extends ListGenerator> listGenerator = dropDownList.listGenerator();
            String param = dropDownList.param();
            ListGenerator bean = applicationContext.getBean(listGenerator);
            List list = bean.generateList(param);
            Optional.ofNullable(parameterModel.getSchema()).map(schema -> schema.getEnum()).ifPresent(l -> list.addAll(l));
            parameterModel.getSchema().setEnum(list);
        }
        return parameterModel;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
