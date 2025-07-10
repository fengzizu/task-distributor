package com.zfq.common.taskdistributor.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ErrorHandler {

    void handleError(ConsumerRecord consumerRecord, Throwable ex);

}