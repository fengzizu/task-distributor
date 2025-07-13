package com.zfq.common.taskdistributor.merge;

import java.util.List;

public interface Directory {

    List< MergeFile> listFiles();

    void deleteIfExist();

    void createIfNotExist();

    boolean exist( );

    String serialize();

}
