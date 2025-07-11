package com.zfq.common.taskdistributor.validator;

public interface Validator<T, M> {

    ValidateResult validate(ValidatorInput<T, M> input);
    boolean isValidate(ValidatorInput<T, M> input);
}
