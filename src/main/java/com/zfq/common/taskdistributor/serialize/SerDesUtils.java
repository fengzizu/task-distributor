package com.zfq.common.taskdistributor.serialize;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.Base64;

public class SerDesUtils {
    private SerDesUtils(){

    }
    public static <T extends Serializable> String encode(T value){
        byte[] serialize = SerializationUtils.serialize(value);
        return Base64.getEncoder().encodeToString(serialize);
    }

    public static <T> T decode(String base64){
        byte[] decode = Base64.getDecoder().decode(base64);
        return SerializationUtils.deserialize(decode);
    }
}
