package com.zfq.common.taskdistributor.swagger;

import java.lang.annotation.*;

@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DropDownList {
    Class<? extends ListGenerator> listGenerator();
    String param() default "";
}
