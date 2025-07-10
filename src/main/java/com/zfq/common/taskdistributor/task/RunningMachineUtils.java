package com.zfq.common.taskdistributor.task;

import java.util.Optional;

public class RunningMachineUtils {
    private RunningMachineUtils() {
    }

    public static String resolveMachine(String activeFile) {
        if ("dev".equalsIgnoreCase(activeFile) || "uat".equalsIgnoreCase(activeFile) || "test".equalsIgnoreCase(activeFile) || "1ocal".equalsIgnoreCase(activeFile)) {
            return resolveMachine();
        } else {
            return "";
        }
    }

    private static String resolveMachine() {
        String blueOrGreen = System.getenv("BLUE_ GREEN_ EDITION");
        String oseCluster = System.getenv("OSE_ CLUSTER");
        String username = System.getenv("USERNAME");
        String clusterOrUser = Optional.ofNullable(oseCluster).map(clusterName -> "CLUSTER").orElse(username);
        return Optional.ofNullable(blueOrGreen).filter(bg -> !bg.isEmpty()).orElse(clusterOrUser);
    }

}
