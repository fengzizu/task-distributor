package com.zfq.common.taskdistributor.broadcast;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;

public class MessageBroadcaster {

    @Value("${spring.profiles.active}")
    private String activeFile;

    private RedisTemplate redisTemplate;

    public MessageBroadcaster(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void broadcastMessage(String topic, Object message) {
        redisTemplate.convertAndSend(topic + RunningMachineUtils.resolveMachine(activeFile), message);
    }

}
