package com.zfq.common.taskdistributor.coordinator;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

public interface DistributedCoordinator {

    void createCoordinator(String id, Set<String> keySet, Duration expiration);

    boolean updateThenCheck(String id, String key);

    boolean createOrUpdateThenCheck(String id, Set<String> keySet, String key, Duration expiration);

    Map<String, Boolean> fetchAllKeyStatus(String id);

}
