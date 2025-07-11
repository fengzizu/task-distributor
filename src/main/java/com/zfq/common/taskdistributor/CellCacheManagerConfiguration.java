package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.cache.DefaultCacheManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.core.RedisTemplate;

import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@Slf4j
public class CellCacheManagerConfiguration {

    @Bean("cellDefaultCacheManager")
    @Primary
    public DefaultCacheManager defaultCacheManager(@Qualifier(CELL_REDIS_TEMPLATE) RedisTemplate redisTemplate){
        return new DefaultCacheManager(redisTemplate);
    }

}
