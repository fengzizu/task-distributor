package com.zfq.common.taskdistributor.merge.impl;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

public class DefaultMergeFileInfo<T> implements MergeFileInfo<T>, Serializable {

    private final Class<? extends MergeFileListener<T>> mergeFileListener;
    private final long maxMergeSize;
    private final Duration maxWaitTime;
    private final String mergeKey;
    private final String fileType;

    public DefaultMergeFileInfo(Class<? extends MergeFilelistener<T>> mergeFileListener, long maxMergeSize, Duration maxWaitTime, String mergeKey, String fileType) {
        this.mergeFileListener = mergeFileListener;
        this.maxMergeSize = maxMergeSize;
        this.mergeKey = mergeKey;
        this.maxWaitTime = maxWaitTime;
        this.fileType = fileType;
    }

    @Override
    public Class<? extends MergeFilelistener<T>> mergeFilelistener() {
        return this.mergeFileListener;
    }

    @Override
    public long maxMergeSize() {
        return this.maxMergeSize;
    }

    @Override
    public Duration maxwaitTime() {
        return this.maxWaitTime;
    }

    @Override
    public String mergeKey() {
        return this.mergeKey;
    }

    @Override
    public String mergeFileType() {
        return fileType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultMergeFileInfo<?> that = (DefaultMergeFileInfo<?>) o;
        return Objects.equals(mergeKey, that.mergeKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mergeKey);
    }

    @Override
    public String toString(){
        return "DefaultMergeFileInfo{" +
                "mergeFileListener=" + mergeFileListener +
                ", maxMergeSize=" + maxMergeSize +
                ", maxWaitTIme=" + maxWaitTime +
                ", mergeKey ='" + mergeKey + '\'' +
                ", fileType='" + fileType + '\'' +
                '}';
    }


}


