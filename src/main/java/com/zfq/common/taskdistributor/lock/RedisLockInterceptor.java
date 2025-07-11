package com.zfq.common.taskdistributor.lock;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;

@Aspect
@Slf4j
public class RedisLockInterceptor implements EnvironmentAware {

    public static final String LOCK_VALUE = "lock";

    static final ScheduledExecutorService POOL = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    @Qualifier(CELL_REDIS_TEMPLATE)
    private RedisTemplate redisTemplate;

    private Environment environment;

    @Around("@annotation(com.zfq.common.taskdistributor.lock.RedisLock)")
    public Object lock(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        RedisLock redisLock = signature.getMethod().getAnnotation(RedisLock.class);
        String lockKey = environment.resolvePlaceholders(redisLock.lockKey());
        long timeout = redisLock.timeout();
        long currentTime = System.currentTimeMillis();
        boolean isSuccess = redisTemplate.opsForZSet().addIfAbsent(lockKey, LOCK_VALUE, currentTime + timeout);
        Object result = null;
        if (isSuccess) {
            log.debug("get lock for key {}", lockKey);
            LockTask lockTask = new LockTask(timeout, lockKey, redisTemplate, POOL);
            POOL.execute(lockTask);
            try {
                result = joinPoint.proceed();
            } catch (Throwable throwable) {
                log.error("redis lock interceptor error. ", throwable);
            } finally {
                lockTask.done();
                redisTemplate.opsForZSet().remove(lockKey, LOCK_VALUE);
            }
        } else {
            log.debug("not get lock for key {}. will retry", lockKey);
        }
        redisTemplate.opsForZSet().removeRangeByScore(lockKey, -1, currentTime);
        return result;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

}
