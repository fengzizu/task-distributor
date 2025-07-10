package com.zfq.common.taskdistributor.broadcast;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.Topic;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import static com.zfq.common.taskdistributor.CellRedisTemplateConfiguration.CELL_REDIS_TEMPLATE;

public abstract class BroadcastListener<T> implements MessageListener {

    public static final String CELL_BROADCAST_TOPIC = "cell_broadcast_topic";

    @Autowired
    @Qualifier(CELL_REDIS_TEMPLATE)
    private RedisTemplate redisTemplate;

    @SneakyThrows
    @Override
    public final void onMessage(Message message, byte[] pattern) {
        onMessage((T) redisTemplate.getValueSerializer().deserialize(message.getBody()), new String(message.getChannel()));
    }

    private Class<T> parseGenericClass() {
        Type[] genericInterfaces = getClass().getGenericInterfaces();
        for (Type genericInterface : genericInterfaces) {
            ParameterizedType it = (ParameterizedType) genericInterface;
            if (it.getRawType().equals(BroadcastListener.class)) {
                Type[] actualTypeArguments = it.getActualTypeArguments();
                return (Class<T>) actualTypeArguments[0];
            }
        }

        return (Class<T>) Object.class;
    }

    public abstract void onMessage(T message, String topic);

    public abstract List<Topic> getTopics();

}
