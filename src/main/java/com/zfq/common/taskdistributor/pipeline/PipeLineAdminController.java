package com.zfq.common.taskdistributor.pipeline;

import com.zfq.common.taskdistributor.task.TaskStatus;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Set;

@RequestMapping("/pipeline")
public class PipeLineAdminController extends BaseController {

    private final PipeLineAdmin pipeLineAdmin;

    public PipeLineAdminController(PipeLineAdmin pipeLineAdmin) {
        this.pipeLineAdmin = pipeLineAdmin;
    }

    @PostMapping("/seekToBegin")
    @Operation(summary = "be careful when using this", description = "be careful when using this")
    public String seekToBegin() {
        pipeLineAdmin.seekToBegin();
        return TaskStatus.SUCCESS.name();
    }

    @PostMapping("/seekToEnd")
    public String seekToEnd() {
        pipeLineAdmin.seekToEnd();
        return TaskStatus.SUCCESS.name();
    }

    @PostMapping("/seekToBeginPerTopic")
    @Operation(summary = "be careful when using this", description = "be careful when using this")
    public String seekToBegin(String topic) {
        pipeLineAdmin.seekToBegin(Set.of(topic));
        return TaskStatus.SUCCESS.name();
    }

    @PostMapping("/seekToEndPerTopic")
    public String seekToEnd(String topic) {
        pipeLineAdmin.seekToEnd(Set.of(topic));
        return TaskStatus.SUCCESS.name();
    }

    @PostMapping("/seekToOffset")
    public String seekToOffset(String topic, Integer partition, Long offset) {
        pipeLineAdmin.seekToOffset(Set.of(new TopicPartitionOffset(topic, partition, offset, null)));
        return TaskStatus.SUCCESS.name();
    }

    @PostMapping("/seekToTimeStamp")
    public String seekToTimeStamp(String topic, Integer partition, Long timestamp) {
        pipeLineAdmin.seekToOffset(Set.of(new TopicPartitionOffset(topic, partition, null, timestamp)));
        return TaskStatus.SUCCESS.name();
    }

}