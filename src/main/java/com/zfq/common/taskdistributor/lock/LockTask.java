package com.zfq.common.taskdistributor.lock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class LockTask implements Runnable {

    private final long timeout;

    private final AtomicBoolean isDone;

    private final String lockKey;

    private final RedisTemplate redisTemplate;

    private final ScheduledExecutorService pool;

    public LockTask(long timeout, String lockKey, RedisTemplate redisTemplate, ScheduledExecutorService pool) {
        this.timeout = timeout;
        this.isDone = new AtomicBoolean(false);
        this.lockKey = lockKey;
        this.redisTemplate = redisTemplate;
        this.pool = pool;
    }

    @Override
    public void run() {
        if (isDone.get()) {
            log.debug("complete extend time for redis lock {}", lockKey);
        } else {
            redisTemplate.opsForZSet().add(lockKey, RedisLockInterceptor.LOCK_VALUE, System.currentTimeMillis() + timeout);
            pool.schedule(this, timeout / 5, TimeUnit.MILLISECONDS);
            log.debug("extend time for redis lock {} by {} ms", lockKey, timeout);
        }
    }

    public void done() {
        this.isDone.set(true);
    }

}
