package com.zfq.common.taskdistributor.validator;

import java.util.List;
import java.util.stream.Collectors;

public interface ValidatorRouter<T extends ValidatorInput> {
    ValidatorManager validatorManager();

    default List<ValidateResult> validate(T input) {
        ValidatorManager validatorManager = validatorManager();
        List<Validator> validators = validatorManager.validators();
        return validators.stream().filter(validator -> validator.isValidate(input))
                .map(validator -> validator.validate(input)).collect(Collectors.toList());

    }
}
