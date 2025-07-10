package com.zfq.common.taskdistributor;

import com.zfq.common.taskdistributor.broadcast.BroadcastListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import java.util.List;
import java.util.stream.Collectors;

@EnableAspectJAutoProxy
@PropertySource("cell.yml")
@Slf4j
public class CellBroadcastConfiguration {

    @Bean("cellMessageBroadcaster")
    public MessageBroadcaster
    messageBroadcaster(@Qualifier(CELL_REDIS_TEMPLATE) RedisTemplate redisTemplate) {
        return new MessageBroadcaster(redisTemplate);
    }

    @Bean("cellRedisMessageListenerContainer")
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory
                                                                               connectionFactory,
                                                                       List<BroadcastListener> broadcastListenerList,
                                                                       @Value("${spring.profiles.active}") String activeFile) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        broadcastListenerList.forEach(listener -> {
            MessageListenerAdapter adapter = new MessageListenerAdapter(listener, "onMessage");
            List<Topic> topics = listener.getTopics();
            List<Topic> isolationTopics = topics.stream().map(t -> new ChannelTopic(t.getTopic() +
                    RunningMachineUtils.resolveMachine(activeFile))).collect(Collectors.toList());
            container.addMessageListener(adapter, isolationTopics);
            log.info("register broadcast listener {} for topic {}", listener.getClass(), isolationTopics);
        });
        return container;
    }

}
