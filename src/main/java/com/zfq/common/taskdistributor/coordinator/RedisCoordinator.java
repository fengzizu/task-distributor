package com.zfq.common.taskdistributor.coordinator;

import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class RedisCoordinator implements DistributedCoordinator {

    private final RedisTemplate redisTemplate;

    public RedisCoordinator(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void createCoordinator(String id, Set<String> keySet, Duration expiration) {
        Set<DefaultTypedTuple> collect = keySet.stream().map(k -> new DefaultTypedTuple(k,
                TaskStatus.RUNNING.getScore())).collect(Collectors.toSet());
        redisTemplate.opsForZSet().addIfAbsent(id, collect);
        redisTemplate.expire(id, expiration);
    }

    @Override
    public boolean updateThenCheck(String id, String key) {
        DefaultRedisScript script = new DefaultRedisScript(
                "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])" +
                        "local count = redis.call('ZCARD', KEYS[1])" +
                        "local successCount = redis.call('ZCOUNT', KEYS[1], 0.0001, 10000000.00)" +
                        "if(count == successCount) then return true else return false end"
        );
        script.setResultType(Boolean.class);
        return (boolean) redisTemplate.execute(script, Arrays.asList(id), key, TaskStatus.SUCCESS.getScore());
    }

    @Override
    public boolean createOrUpdateThenCheck(String id, Set<String> keySet, String key, Duration expiration) {
        Set<DefaultTypedTuple> collect = keySet.stream().map(k -> new DefaultTypedTuple(k,
                TaskStatus.RUNNING.getScorel)).collect(Collectors.toSet());

        Long count = redisTemplate.opsForZSet().addIfAbsent(id, collect);
        Optional.ofNullable(count).filter(c -> c == keySet.size()).ifPresent(created -> redisTemplate.expire(id, expiration));
        DefaultRedisScript script = new DefaultRedisScript(
                "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])" +
                        "local count = redis.call('ZCARD', KEYS[1]) " +
                        "local successCount = redis.call('ZCOUNT', KEYS[1], 0.0001, 10000000.00)" +
                        "if(count == successCount) then return true else return false end"
        );
        script.setResultType(Boolean.class);
        return (boolean) redisTemplate.execute(script, Arrays.asList(id), key, TaskStatus.SUCCESS.getScore());
    }

    @Override
    public Map<String, Boolean> fetchAllKeyStatus(String id) {
        Set<ZSetOperations.TypedTuple> set = redisTemplate.opsForZSet().rangeWithScores(id, 0, -1);
        Map<String, Boolean> result = new HashMap<>(set.size());
        set.forEach(t -> result.put((String) t.getValue(), t.getScore() > 0));
        return result;
    }

}
