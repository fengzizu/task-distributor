package com.zfq.common.taskdistributor.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zfq.common.taskdistributor.event.CellStatusEventListener;
import com.zfq.common.taskdistributor.event.RootTaskStatusEvent;
import com.zfq.common.taskdistributor.task.ProcessorTask;
import com.zfq.common.taskdistributor.task.TaskOutput;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public class StatusEventProcessor implements Processor {

    @Autowired(required = false)
    private List<CellStatusEventListener> cellStatusEventListenerList;

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public TaskOutput process(ProcessorTask processorTask) {
        if (cellStatusEventListenerList != null) {
            String payload = processorTask.getPayloadMeta().get(RootTaskStatusEvent.class.getName());
            try {
                RootTaskStatusEvent rootTaskStatusEvent = objectMapper.readValue(payload, RootTaskStatusEvent.class);
                for (CellStatusEventListener cellStatusEventListener : cellStatusEventListenerList) {
                    cellStatusEventListener.onEvent(rootTaskStatusEvent);
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * return TaskOutput without any status as this task no need to update status
         * if update status then it will keep in the loop of send status event -> process event -> update status -> send status event
         */
        return new TaskOutput();
    }

}
