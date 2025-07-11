package com.zfq.common.taskdistributor.cache;

import org.springframework.data.redis.core.RedisTemplate;

public class DefaultCacheManager<T> extends CacheManager<T> {

    public DefaultCacheManager(RedisTemplate redisTemplate) {
        super(redisTemplate);
    }

    @Override
    protected int getMaxCacheSize() {
        return 10000;
    }

}
