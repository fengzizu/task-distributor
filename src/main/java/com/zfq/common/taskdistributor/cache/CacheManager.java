package com.zfq.common.taskdistributor.cache;

import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class CacheManager<T> {

    private static final ScheduledExecutorService CACHE_MANAGER_POOL = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("cell-cache-manager-"));

    private RedisTemplate redisTemplate;
    private AtomicInteger taskAmount = new AtomicInteger(0);

    public CacheManager(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public final void addCache(String key, T value, Duration expireTime) {
        long currentTimeMillis = System.currentTimeMillis();
        redisTemplate.opsForZSet().add(key, value, currentTimeMillis + expireTime.toMillis());
        addRemoveTaskAccordingly(key, expireTime);
    }

    private void addRemoveTaskAccordingly(String key, Duration expireTime) {
        if (taskAmount.incrementAndGet() <= getMaxCacheSize()) {
            CACHE_MANAGER_POOL.schedule(() -> removeIfNeeded(key), expireTime.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            removeIfNeeded(key);
        }
    }

    private void removeIfNeeded(String key) {
        redisTemplate.opsForZSet().removeRangeByScore(key, 0, System.currentTimeMillis());
        redisTemplate.opsForZSet().removeRange(key, getMaxCacheSize(), -1);
        taskAmount.decrementAndGet();
    }

    public final void addCaches(String key, List<T> valueList, Duration expireTime) {
        long currentTimeMillis = System.currentTimeMillis();
        long score = currentTimeMillis + expireTime.toMillis();
        Set<DefaultTypedTuple> collect = valueList.stream().map(v -> new DefaultTypedTuple(v, (double)
                score)).collect(Collectors.toSet());
        redisTemplate.opsForZSet().add(key, collect);
        addRemoveTaskAccordingly(key, expireTime);
    }

    public final Set<T> fetchCache(String key) {
        return redisTemplate.opsForZSet().rangeByScore(key, System.currentTimeMillis(), Double.MAX_VALUE);
    }

    public final void removeCache(String key, T... removeValue) {
        redisTemplate.opsForZSet().remove(key, removeValue);
    }

    protected abstract int getMaxCacheSize();

}