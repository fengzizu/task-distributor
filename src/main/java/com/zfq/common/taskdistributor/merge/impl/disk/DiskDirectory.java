package com.zfq.common.taskdistributor.merge.impl.disk;

import com.zfq.common.taskdistributor.merge.Directory;
import com.zfq.common.taskdistributor.merge.MergeFile;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public record DiskDirectory(File directory) implements Directory {

    @Override
    public List<MergeFile> listFiles() {
        File[] files = directory.listFiles();
        if (files != null) {
            return Arrays.asList(files).stream().map(file -> new DiskMergeFile(file))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void deleteIfExist() {
        directory.delete();
    }


    @Override
    public void createIfNotExist() {
        directory.mkdirs();
    }

    @Override
    public boolean exist() {
        return true;
    }

    @Override
    public String serialize() {
        return directory.getAbsolutePath();
    }

}
