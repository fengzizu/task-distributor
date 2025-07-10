package com.zfq.common.taskdistributor.validator;

import java.util.List;

public interface ValidatorManager {

    List<Validator> validators();
    void save(Validator validator);
    void update(Validator validator);
    void delete(Validator validator);
}
