package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.merge.MergeFileInfo;

import java.time.Duration;

public class ExtendedMergeFileInfo extends DefaultMergeFileInfo{

    private final String uniqueFileName;
    public ExtendedMergeFileInfo(MergeFileInfo mergeFileInfo, String uniqueFileName) {
        this(mergeFileInfo.mergeFileListener(), mergeFileInfo.maxMergeSize(), mergeFileInfo.maxWaitTime(), mergeFileInfo.mergeFileType(), mergeFileInfo.mergeFileType(), uniqueFileName);
    }
    public ExtendedMergeFileInfo(Class mergeFileListener, long maxMergeSize, Duration maxWaitTime, String mergeKey, String fileType, String uniqueFileName) {
        super(mergeFileListener, maxMergeSize, maxWaitTime, mergeKey, fileType);
        this.uniqueFileName = uniqueFileName;

    }
    public String getUniqueFileName() {
        return uniqueFileName;
    }

}
