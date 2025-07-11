package com.zfq.common.taskdistributor.processor;

public interface Processor {

    TaskOutput process(ProcessorTask processorTask);

    default void onError(ProcessorTask processorTask, Throwable throwable) {
    }

    default String group() {
        return "";
    }

    /**
     * worker count
     * if less than 1 then use shared worker
     * else use exclusive worker
     *
     * @return
     */
    default int workerCount() {
        return 0;
    }

}
