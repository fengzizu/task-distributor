package com.zfq.common.taskdistributor.merge.impl.disk;

import com.zfq.common.taskdistributor.merge.FileMerger;
import com.zfq.common.taskdistributor.merge.FileMergerTestament;
import com.zfq.common.taskdistributor.merge.impl.AbstractTestamentProcessor;

import java.io.File;

public class DiskTestamentProcessor extends AbstractTestamentProcessor {

    public DiskTestamentProcessor(FileMerger fileMerger, String group) {
        super(fileMerger, group);
    }

    @Override
    public FileMergerTestament deserialize(String text) {
        return () -> new DiskDirectory(new File(text));
    }

}
