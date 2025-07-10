package com.zfq.common.taskdistributor.task;

import lombok.Generated;
import lombok.Getter;

@Getter
@Generated
public enum TaskStatus {
    RUNNING(0.0), SUCCESS(1.0), FAIL(-1.0);

    private final double score;

    TaskStatus(double score){
        this.score = score;
    }

    public static TaskStatus fromScore(Double score){
        if(score > 0){
            return SUCCESS;
        } else if(score < 0){
            return FAIL;
        } else{
            return RUNNING;
        }
    }

}
