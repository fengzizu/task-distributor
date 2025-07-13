package com.zfq.common.taskdistributor.merge;

import java.util.stream.Stream;

public interface MergeFileListener<T> {

    void onMergeFile(ExtendedMergeFileInfo mergeFileInfo, Stream<T> mergedLines);

    T deserialize(byte[] line);

}
