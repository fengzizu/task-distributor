package com.zfq.common.taskdistributor.merge;

import java.io.IOException;
import java.util.List;

public interface MergeFile<T> {

    long size();

    void append(byte[] data) throws IOException;

    List< byte[]> readLines();

    void delete();

    long lastUpdateTime();

    MergeFileInfo<T> mergeFileInfo();

    String uniqueFileName();

}
