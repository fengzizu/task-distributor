package com.zfq.common.taskdistributor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.core.RedisTemplate;

import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@ConditionalOnClass(name = {"com.citi.sphinx.common.spring.redis.config SphinxRedisConfig", "com.citi.sphinx.common.spring.redis.config.RedisTemplateConfig"})
@Slf4j
public class CellSphinxRedisTemplateConfiguration {

    @Bean(CELL_REDIS_TEMPLATE)
    @ConditionalOnBean(name = "sphinxRedisTemplate")
    public RedisTemplate cellSphinxRedisTemplate(@Qualifier("sphinxRedisTemplate") RedisTemplate redisTemplate) {
        log.info("using sphinxRedisTemplate");
        return redisTemplate;
    }

}
