package com.zfq.common.taskdistributor.merge;

import java.util.stream.Stream;

public interface MergeFileWriter<T extends MergeFile> {

    void writeFile(T mergeFile);

    void writeFiles(T sample, Stream<MergeFile> mergeFileStream);

}
