package com.zfq.common.taskdistributor.validator;

public interface ValidatorInput<T, M>{
    M meta();
    T origin();
    T current();
}
