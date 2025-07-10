package com.zfq.common.taskdistributor;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@ImportAutoConfiguration(CellSphinxRedisTemplateConfiguration.class)
@Slf4j
public class CellRedisTemplateConfiguration {

    public static final String CELL_REDIS_TEMPLATE = "cellRedisTemplate";

    @Bean(CELL_REDIS_TEMPLATE)
    @ConditionalOnMissingBean(name = CELL_REDIS_TEMPLATE)
    @ConditionalOnSingleCandidate(RedisConnectionFactory.class)
    public RedisTemplate cellRedisTemplate(RedisConnectionFactory redisConnectionFactory, Jackson2ObjectMapperBuilder builder) {
        log.info("using default RedisConnectionFactory to create RedisTemplate");
        ObjectMapper objectMapper = builder.build();
        objectMapper.activateDefaultTyping(objectMapper.getPolymorphicTypeValidator(),
                ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
        RedisTemplate<String, Object> template = createTemplate(redisConnectionFactory, objectMapper);
        template.afterPropertiesSet();
        return template;
    }

    private RedisTemplate<String, Object> createTemplate(
            RedisConnectionFactory redisConnectionFactory, ObjectMapper objectMapper
    ) {
        GenericJackson2JsonRedisSerializer genericJackson2JsonRedisSerializer =
                new GenericJackson2JsonRedisSerializer(objectMapper);
        RedisTemplate<String, Object> template = new RedisTemplate();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(StringRedisSerializer.UTF_8);
        template.setValueSerializer(genericJackson2JsonRedisSerializer);
        template.setHashKeySerializer(StringRedisSerializer.UTF_8);
        template.setHashValueSerializer(genericJackson2JsonRedisSerializer);
        return template;
    }

}
