package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.merge.MergeFile;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultMergeFileWriter implements MergeFileWriter<MergeFile> {
    private final ApplicationContext context;

    public DefaultMergeFileWriter(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public void writeFile(MergeFile mergeFile) {
        Class<? extends MergeFileListener> listener = mergeFile.mergeFileInfo().mergeFilelistener();
        MergeFileListener mergeFileListener = context.getBean(listener);
        List<byte[]> list = mergeFile.readlines();
        List collect = list.stream().map(s -> mergeFileListener.deserialize(s)).collect(Collectors.toList());
        mergeFilelistener.onMergeFile(new ExtendedMergeFileInfo(mergeFile.mergeFileInfo(), mergeFile.uniqueFileName()), collect.stream());
    }

    @Override
    public void writeFiles(MergeFile sample, Stream<MergeFile> mergeFileStream) {
        Class<? extends MergeFileListener> requiredType = sample.mergeFileInfo().mergeFileListener();
        MergeFileListener listener = context.getBean(requiredType);
        Stream stream = mergeFileStream.flatMap(mergeFile -> mergeFile.readLines().stream().map(line -> listener.deserialize((byte[]) line)));
        listener.onMergeFile(new ExtendedMergeFileInfo(sample.mergeFileInfo(), sample.uniqueFileName()), stream);
    }

}
