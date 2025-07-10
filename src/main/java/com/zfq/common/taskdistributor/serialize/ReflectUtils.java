package com.zfq.common.taskdistributor.serialize;

import com.sun.jdi.ClassType;
import com.zfq.common.taskdistributor.merge.Map_List.group.RegisterInfo;
import com.zfq.common.taskdistributor.merge.MergeFileWriter;

import java.beans.Introspector;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;


public class ReflectUtils {

    public static final String GET = "get";
    public static final String IS = "is";

    private ReflectUtils() {
    }

    public static void main(String[] args) {
        String groupFoldName = parseFoldName(RegisterInfo::getGroup);
        System.out.println(groupFoldName);
        String boolFoldName = parseFoldName(RegisterInfo::isCommitted);
        System.out.println(boolFoldName);
        RegisterInfo registerInfo = new RegisterInfo("abc", true);
        String test = "test";
        String encoderTest = SerDesUtils.encode(test);
        Object decoderTest = SerDesUtils.encode(encoderTest);
        setvalue(groupFoldName, decoderTest, registerInfo);
        boolean value = false;
        String encoderValue = SerDesUtils.encode(value);
        Object decoderValue = SerDesUtils.encode(encoderValue);
        setValue(boolFoldName, decoderValue, registerInfo);
        System.out.println(registerInfo.getGroup());
        System.out.println(registerInfo.isCommitted());
        List<Type> list = genericTypes(ListFileMerger.class, FileMerger.class);
        list.forEach(System.out::println);
        List<Type> types = genericTypes(ListFileMerger.class, AbstractFileMerger.class);
        types.forEach(System.out::println);
        List<Type> genericTypes = genericTypes(DefaultMergeFileWriter.class, MergeFileWriter.class);
        genericTypes.forEach(System.out::println);
    }

    public static <T, R> String parseFoldName(SerializableFunction<T, R> function) {
        try {
            Method writeReplace = function.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            SerializedLambda lambda = (SerializedLambda) writeReplace.invoke(function);
            String methodName = lambda.getImplMethodName();
            String prefix = "";
            if (methodName.startsWith(GET)) {
                prefix = GET;
            } else if (methodName.startsWith(IS)) {
                prefix = IS;
            }
            return Introspector.decapitalize(methodName.replaceFirst(prefix, ""));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <V, T> void setValue(String fieldName, V value, T target) {
        try {
            Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Type> genericTypes(Class implementsClass, Class genericClass) {
        if (genericClass.isAssignableFrom(implementsClass)) {
            if (genericClass.isInterface()) {
                Type[] genericInterfaces = implementsClass.getGenericInterfaces();
                for (Type anInterface : genericInterfaces) {
                    if (anInterface instanceof ParameterizedType parameterizedType) {
                        Type rawType = parameterizedType.getRawType();
                        if (rawType instanceof Class<?> clazz) {
                            if (genericClass.isAssignableFrom(clazz)) {
                                return getClasses(parameterizedType);
                            }
                        }
                    }
                }
                Class superClass = implementsClass.getSuperclass();
                if (superClass != null) {
                    return genericTypes(superClass, genericClass);
                } else {
                    Type genericSuperClass = implementsClass.getGenericSuperclass();
                    if (genericSuperClass instanceof ParameterizedType parameterizedType) {
                        return getClasses(parameterizedType);
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    private static List<Type> getClasses(ParameterizedType parameterizedType) {
        Type[] actuallyTypeArgs = parameterizedType.getActualTypeArguments();
        List<Type> list = new ArrayList<>(actuallyTypeArgs.length);
        for (Type actuallyTypeArg : actuallyTypeArgs) {
            list.add(actuallyTypeArg);
        }
        return list;
    }

    public static interface SerializableFunction<T, R> extends Function<T, R>, Serializable {

    }
}
