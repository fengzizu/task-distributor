package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.coordinator.RedisCoordinator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.core.RedisTemplate;

import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;

@EnableAspectJAutoProxy
@PropertySource("cell. yml")
@AutoConfigureAfter({CellAutoConfiguration.class})
public class CellCoordinatorConfig {

    @Bean("cellCoordinator")
    @ConditionalOnMissingBean
    private RedisCoordinator redisCoordinator(@Qualifier(CELL_REDIS_TEMPLATE) RedisTemplate redisTemplate) {
        return new RedisCoordinator(redisTemplate);
    }

}
