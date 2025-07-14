package com.zfq.common.taskdistributor.merge;

import java.io.Serializable;
import java.time.Duration;

public interface MergeFileInfo<T> extends Serializable {

    Class<? extends MergeFileListener<T>> mergeFileListener( );

    long maxMergeSize();

    Duration maxWaitTime();

    String mergeKey();

    String mergeFileType();

}
