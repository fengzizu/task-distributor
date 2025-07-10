package com.zfq.common.taskdistributor.validator;

public interface ValidateResult {
    Validator validator();
    boolean isPass();
    String message();
}
