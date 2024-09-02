package org.apache.doris.common.jni.utils;

import org.apache.doris.thrift.TFunction;

import com.esotericsoftware.reflectasm.MethodAccess;

import java.lang.reflect.Method;

public class UdfClassCache {
    public Class<?> c;
    public MethodAccess methodAccess;
    public int evaluateIndex;
    public JavaUdfDataType[] argTypes;
    public JavaUdfDataType retType;
    public Class[] argClass;
    public TFunction fn;
    public Method method;
    public Method prepareMethod;
}
