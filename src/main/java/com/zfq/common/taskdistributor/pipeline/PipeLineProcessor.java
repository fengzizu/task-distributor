package com.zfq.common.taskdistributor.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PipeLineProcessor implements MessageProcessor {

    private final MessageTransformer messageTransformer;

    private final KafkaTemplate kafkaTemplate;

    public PipeLineProcessor(MessageTransformer messageTransformer, KafkaTemplate kafkaTemplate) {
        this.messageTransformer = messageTransformer;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletableFuture process(ConsumerRecord record) {
        List<ProducerRecord> transform = messageTransformer.process(record);
        return sendResult(transform);
    }

    public CompletableFuture sendResult(List<ProducerRecord> list) {
        try {
            if (CollectionUtils.isEmpty(list)) {
                return CompletableFuture.completedFuture(null);
            } else {
                CompletableFuture[] completableFutures = new CompletableFuture[list.size()];
                for (int i = 0; i < list.size(); i++) {
                    completableFutures[i] = kafkaTemplate.send(list.get(i));
                }
                return CompletableFuture.allOf(completableFutures);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public MessageTransformer getMessageTransformer() {
        return messageTransformer;
    }

    public KafkaTemplate getKafkaTemplate() {
        return kafkaTemplate;
    }

}
