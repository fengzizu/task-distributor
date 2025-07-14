package com.zfq.common.taskdistributor.merge;

import com.zfq.common.taskdistributor.merge.impl.DefaultMergeFileInfo;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.util.Map;

public interface FileMerger<F extends MergeFile, W extends MergeFileWriter, R extends MergeFileManager, D extends Directory> extends SmartLifecycle {

    void merge(MergeFileInfo mergeFileInfo, byte[] line) throws IOException;

    F currentMergeFile(MergeFileInfo mergeFileInfo);

    Map<MergeFileInfo, F> currentMergeFiles();

    void publishTestament(FileMergerTestament testament);

    default void writeAndDelete(F mergeFile) {
        fileWriter().writeFile(mergeFile);
        mergeFile.delete();
    }

    default void removeAndWriteAndDelete(MergeFileInfo mergeFileInfo) {
        F remove = remove(mergeFileInfo);
        writeAndDelete(remove);
    }

    default F remove(MergeFileInfo mergeFileInfo) {
        return currentMergeFiles().remove(mergeFileInfo);
    }

    default void removeAndCommit(String mergeKey) {
        MergeFileInfo mergeFileInfo = new DefaultMergeFileInfo(null, 1L, null, mergeKey, null);
        F remove = remove(mergeFileInfo);
        if (remove != null) {
            mergeFileManager().commit(remove);
        }
    }

    W fileWriter();

    R mergeFileManager();

    F createMergeFile(MergeFileInfo mergeFileInfo);

    void manageTimeoutMergeFile();

    FileMergerTestament createTestament();

    Directory workingDirectory();

}
