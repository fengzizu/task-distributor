package com.zfq.common.taskdistributor.pipeline;

import com.zfq.common.taskdistributor.broadcast.BroadcastListener;
import com.zfq.common.taskdistributor.broadcast.MessageBroadcaster;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.Topic;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeLineAdmin extends BroadcastListener<SeekEvent> {
    private final List<PipeLine> pipeLineList;

    private final String broadCasterTopic;
    private final List<Topic> topics;

    private final MessageBroadcaster messageBroadcaster;

    public PipeLineAdmin(List<PipeLine> pipeLineList, String broadCasterTopic, MessageBroadcaster messageBroadcaster) {
        this.pipeLineList = pipeLineList;
        this.broadCasterTopic = broadCasterTopic;
        this.topics = List.of(new ChannelTopic(broadCasterTopic));
        this.messageBroadcaster = messageBroadcaster;

    }

    public void seekToBegin() {
        messageBroadcaster.broadcastMessage(broadCasterTopic, new SeekEvent(SeekToType.BEGIN));
    }

    public void seekToEnd() {
        messageBroadcaster.broadcastMessage(broadCasterTopic, new SeekEvent(SeekToType.END));
    }

    public void seekToBegin(Set<String> kafkaTopicSet) {
        Set<TopicPartitionOffset> collect = kafkaTopicSet.stream().map(t -> new TopicPartitionOffset(t, null, null, null)).collect(Collectors.toSet());
        SeekEvent seekEvent = new SeekEvent(SeekToType.BEGIN, collect);
        messageBroadcaster.broadcastMessage(broadCasterTopic, seekEvent);
    }

    public void seekToEnd(Set<String> kafkaTopicSet) {
        Set<TopicPartitionOffset> collect = kafkaTopicSet.stream().map(t -> new TopicPartitionOffset(t, null, null, null)).collect(Collectors.toSet());
        SeekEvent seekEvent = new SeekEvent(SeekToType.END, collect);
        messageBroadcaster.broadcastMessage(broadCasterTopic, seekEvent);
    }

    public void seekToOffset(Set<TopicPartitionOffset> topicPartitionOffsetSet) {
        SeekEvent seekEvent = new SeekEvent(SeekToType.OFFSET, topicPartitionOffsetSet);
        messageBroadcaster.broadcastMessage(broadCasterTopic, seekEvent);
    }

    public void seekToTimeStamp(Set<TopicPartitionOffset> topicPartitionOffsetSet) {
        SeekEvent seekEvent = new SeekEvent(SeekToType.TIMESTAMP, topicPartitionOffsetSet);
        messageBroadcaster.broadcastMessage(broadCasterTopic, seekEvent);
    }

    @Override
    public void onMessage(SeekEvent message, String topic) {
        Set<TopicPartitionOffset> seekToSet = message.getSeekToSet();
        if (SeekToType.BEGIN.equals(message.getSeekToType())) {
            pipeLineList.forEach(pipeline -> pipeline.seekToBegin(seekToSet));
        } else if (SeekToType.END.equals(message.getSeekToType())) {
            pipeLineList.forEach(pipeline -> pipeline.seekToEnd(seekToSet));
        } else if (SeekToType.OFFSET.equals(message.getSeekToType())) {
            pipeLineList.forEach(pipeline -> pipeline.seekToOffset(seekToSet));
        } else if (SeekToType.TIMESTAMP.equals(message.getSeekToType())) {
            pipeLineList.forEach(pipeline -> pipeline.seekToTimeStamp(seekToSet));
        }
    }

    @Override
    public List<Topic> getTopics() {
        return topics;
    }

}

