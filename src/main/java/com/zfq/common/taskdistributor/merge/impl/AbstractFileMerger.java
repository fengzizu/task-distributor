package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.merge.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.IOException;
import java.util.concurrent.*;

@Slf4j
public abstract class AbstractFileMerger<F extends MergeFile, W extends MergeFileWriter, R extends MergeFileManager, D extends Directory> implements FileMerger<F, W, R, D> {

    protected final ConcurrentHashMap<MergeFileInfo, F> currentMergeFiles;

    protected final W writer;

    protected final R committer;

    protected final D workingDirectory;

    private final ScheduledExecutorService MERGE_FILE_MANAGER;

    private final ExecutorService WRITE_POOL;

    private final int writePoolSize;

    private volatile boolean isRunning = false;

    public AbstractFileMerger(W writer, R committer, D workingDirectory, int writePoolSize) {
        this.writer = writer;
        this.committer = committer;
        this.workingDirectory = workingDirectory;
        this.currentMergeFiles = new ConcurrentHashMap<>();
        this.MERGE_FILE_MANAGER = Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("merge-file-manager-"));

        this.writePoolSize = writePoolSize;
        WRITE_POOL = new ThreadPoolExecutor(this.writePoolSize, this.writePoolSize, Integer.MAX_VALUE,
                TimeUnit.DAYS, new SynchronousQueue(),
                new CustomizableThreadFactory("writer-"), new ThreadPoolExecutor.CallerRunsPolicy());
        MERGE_FILE_MANAGER.scheduleAtFixedRate(() -> manageTimeoutMergeFile(), 5, period(), TimeUnit.MINUTES);
    }

    public long period() {
        return 1;
    }

    @Override
    public void merge(MergeFileInfo mergeFileInfo, byte[] line) throws IOException {
        F mergeFile = currentMergeFile(mergeFileInfo);
        /**
         * have to synchronized twice as the merge file may be written and deleted when the thread
         * goes into the first layer of synchronized.
         */
        synchronized (mergeFile) {
            /**
             * To avoid manipulating on the deleted merge file must get again
             */
            mergeFile = currentMergeFile(mergeFileInfo);
            synchronized (mergeFile) {
                mergeFile.append(line);
                long size = mergeFile.size();
                if (size >= mergeFile.mergeFileInfo().maxMergeSize()) {
                    remove(mergeFileInfo);
                    writeAndDelete(mergeFile);
                }
            }
        }
    }

    @Override
    public void manageTimeoutMergeFile() {
        try {
            currentMergeFiles().forEach((mergeFileInfo, mergeFile) -> {
                synchronized (mergeFile) {
                    log.info("check merge file {} timeout or not'", mergeFile.mergeFileInfo().toString());
                    if (System.currentTimeMillis() - mergeFile.lastUpdateTime() > mergeFile.mergeFileInfo().maxWaitTime().toMillis()) {
                        remove(mergeFileInfo);
                        writeAndDelete(mergeFile);
                    }
                }
            });
        } catch (Throwable e) {
            log.error("manage timeout file error.", e);
        }
    }

    @Override
    public F currentMergeFile(MergeFileInfo mergeFileInfo) {
        return currentMergeFiles().computeIfAbsent(mergeFileInfo, fileInfo -> createMergeFile(fileInfo));
    }

    @Override
    public W fileWriter() {
        return writer;
    }

    @Override
    public R mergeFileManager() {
        return committer;
    }

    @Override
    public ConcurrentHashMap<MergeFileInfo, F> currentMergeFiles() {
        return currentMergeFiles;
    }

    @Override
    public Directory workingDirectory() {
        return workingDirectory;
    }

    @Override
    public void start() {
        isRunning = true;
    }

    @Override
    public void stop() {
        MERGE_FILE_MANAGER.shutdown();
        WRITE_POOL.shutdown();
        isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

}

