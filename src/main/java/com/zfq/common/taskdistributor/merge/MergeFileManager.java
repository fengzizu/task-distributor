package com.zfq.common.taskdistributor.merge;

public interface MergeFileManager<T extends MergeFile, R, U> {

    void register(T mergeFile);

    R registerInfo( String mergeKey);

    void commit(T mergeFile);

    void unregister( String mergeKey, U unregister);

}