package com.zfq.common.taskdistributor.pipeline;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class ErrorHandlers {

    public static class LogErrorHandler implements ErrorHandler {
        private final int retryTimes;

        public LogErrorHandler(int retryTimes) {
            this.retryTimes = retryTimes;
        }

        @Override
        public void handleError(ConsumerRecord consumerRecord, Throwable ex) {
            log.error("error process record topic {} partition {} offset {}", consumerRecord.topic(), consumerRecord.partition(),
                    consumerRecord.offset(), ex);
        }
    }

}
