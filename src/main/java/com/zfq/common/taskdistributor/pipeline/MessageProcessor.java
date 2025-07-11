package com.zfq.common.taskdistributor.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;

public interface MessageProcessor<T> {

    CompletableFuture<T> process(ConsumerRecord record);

}
