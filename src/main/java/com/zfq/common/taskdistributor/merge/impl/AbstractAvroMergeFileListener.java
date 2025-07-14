package com.zfq.common.taskdistributor.merge.impl;

import com.zfq.common.taskdistributor.merge.MergeFileListener;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;

public abstract class AbstractAvroMergeFileListener<T> implements MergeFileListener<T> {

    @Override
    public T deserialize(byte[] line) {
        Class<T> tClass = parseGenericClass();
        try {
            Object decoder = tClass.getDeclaredMethod( "getDecoder").invoke(tClass);
            Method decode = decoder.getClass( ).getMethod("decode", byte[].class);
            return (T) decode.invoke(decoder, line);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Class<T> parseGenericClass() {
        return (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

}
