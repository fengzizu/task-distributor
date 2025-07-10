package com.zfq.common.taskdistributor.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RootTaskStatusEvent {

    private String key;

    private TaskStatus taskStatus;

}
