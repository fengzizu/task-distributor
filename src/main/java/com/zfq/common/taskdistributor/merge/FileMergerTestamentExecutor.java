package com.zfq.common.taskdistributor.merge;

public interface FileMergerTestamentExecutor {

    void executeTestament( FileMergerTestament testament);

    FileMerger fileMerger();

    FileMergerTestament deserialize(String text);

}

