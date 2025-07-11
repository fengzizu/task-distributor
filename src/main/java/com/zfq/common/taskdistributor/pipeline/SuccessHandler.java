package com.zfq.common.taskdistributor.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface SuccessHandler<T> {

    void handleSuccess(ConsumerRecord consumerRecord, T result);

}
