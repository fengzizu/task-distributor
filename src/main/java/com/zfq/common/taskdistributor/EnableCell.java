package com.zfq.common.taskdistributor;

import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.context.annotation.PropertySource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ImportAutoConfiguration({CellRedisTemplateConfiguration.class, CellAutoConfiguration.class})
/**
 * the order maters
 */
@PropertySource(name = "cellProperties", value = {"classpath:cell. yml", "classpath:cell-$ {spring.profiles.active}.yml"}, factory = YamlPropertyLoaderFactory.class)
public @interface EnableCell {
}
