package com.zfq.common.taskdistributor.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public interface MessageTransformer {

    List<ProducerRecord> process(ConsumerRecord consumerRecord);

}
