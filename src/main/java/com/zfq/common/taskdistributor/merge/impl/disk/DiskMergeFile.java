package com.zfq.common.taskdistributor.merge.impl.disk;

import com.zfq.common.taskdistributor.merge.MergeFile;
import com.zfq.common.taskdistributor.merge.MergeFileInfo;
import org.apache.commons.lang3.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DiskMergeFile<T> implements MergeFile<T> {

    public static final int LENGTH_INFO_SIZE = 4;

    private final File file;

    private final MergeFileInfo<T> mergeFileInfo;

    private volatile long lastUpdateTime;

    private long size;

    public DiskMergeFile(String pathName) {
        this(new File(pathName));
    }

    public DiskMergeFile(File file) {
        this.file = file;
        this.size = file.length();
        try {
            byte[] bytes = Files.readAllBytes(file.toPath());
            int length = ByteBuffer.wrap(bytes, 0, LENGTH_INFO_SIZE).getInt();
            byte[] array = ByteBuffer.allocate(length).put(bytes, LENGTH_INFO_SIZE, length).array();
            this.mergeFileInfo = SerializationUtils.deserialize(array);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public DiskMergeFile(File file, MergeFileInfo mergeFileInfo) {
        this.file = file;
        this.size = file.length();
        this.lastUpdateTime = System.currentTimeMillis(); .
        this.mergeFileInfo = mergeFileInfo;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public void append(byte[] data) throws IOException {
        int length = data.length;
        this.size = this.size + length;
        byte[] array = ByteBuffer.allocate(LENGTH_INFO_SIZE).putInt(length).array();
        Files.write(file.toPath(), ByteBuffer.allocate(array.length + length).put(array).put(data).array(), StandardOpenOption.APPEND);
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public List<byte[]> readLines() {
        try {
            byte[] bytes = Files.readAllBytes(file.toPath());
            if (bytes.length <= 4) {
                return Collections.emptyList();
            }
            int length = ByteBuffer.wrap(bytes, 0, LENGTH_INFO_SIZE).getInt();
            int currentPosition = LENGTH_INFO_SIZE;
            List<byte[]> lines = new ArrayList<>();
            for (; ; ) {
                byte[] line = new byte[length];
                for (int i = currentPosition; i < currentPosition + length; i++) {
                    line[i - currentPosition] = bytes[i];
                }
                lines.add(line);
                currentPosition = currentPosition + length;
                if (currentPosition >= bytes.length) {
                    return lines.subList(1, lines.size());
                }
                length = ByteBuffer.wrap(bytes, currentPosition, LENGTH_INFO_SIZE).getInt();
                currentPosition = currentPosition + LENGTH_INFO_SIZE;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete() {
        file.delete();
    }

    @Override
    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public MergeFileInfo<T> mergeFileInfo() {
        return this.mergeFileInfo;
    }

    @Override
    public String uniqueFileName() {
        return file.getName();
    }

    public File getFile() {
        return file;
    }

}
