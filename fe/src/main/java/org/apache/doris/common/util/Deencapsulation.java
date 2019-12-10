package org.apache.doris.common.util;

import org.apache.doris.common.ConstructorReflection;
import org.apache.doris.common.FieldReflection;
import org.apache.doris.common.MethodReflection;

public final class Deencapsulation {
    private Deencapsulation() {
    }

    public static <T> T getField(Object objectWithField, String fieldName) {
        return FieldReflection.getField(objectWithField.getClass(), fieldName, objectWithField);
    }

    public static <T> T getField(Object objectWithField, Class<T> fieldType) {
        return FieldReflection.getField(objectWithField.getClass(), fieldType, objectWithField);
    }

    public static <T> T getField(Class<?> classWithStaticField, String fieldName) {
        return FieldReflection.getField(classWithStaticField, fieldName, (Object)null);
    }

    public static <T> T getField(Class<?> classWithStaticField, Class<T> fieldType) {
        return FieldReflection.getField(classWithStaticField, fieldType, (Object)null);
    }

    public static void setField(Object objectWithField, String fieldName, Object fieldValue) {
        FieldReflection.setField(objectWithField.getClass(), objectWithField, fieldName, fieldValue);
    }

    public static void setField(Object objectWithField, Object fieldValue) {
        FieldReflection.setField(objectWithField.getClass(), objectWithField, (String)null, fieldValue);
    }

    public static void setField(Class<?> classWithStaticField, String fieldName, Object fieldValue) {
        FieldReflection.setField(classWithStaticField, (Object)null, fieldName, fieldValue);
    }

    public static void setField(Class<?> classWithStaticField, Object fieldValue) {
        FieldReflection.setField(classWithStaticField, (Object)null, (String)null, fieldValue);
    }

    public static <T> T invoke(Object objectWithMethod, String methodName, Object... nonNullArgs) {
        Class<?> theClass = objectWithMethod.getClass();
        return MethodReflection.invoke(theClass, objectWithMethod, methodName, nonNullArgs);
    }

    public static <T> T invoke(Class<?> classWithStaticMethod, String methodName, Object... nonNullArgs) {
        return MethodReflection.invoke(classWithStaticMethod, (Object)null, methodName, nonNullArgs);
    }

    public static <T> T newInstance(Class<? extends T> classToInstantiate, Object... nonNullArgs) {
        return ConstructorReflection.newInstance(classToInstantiate, nonNullArgs);
    }

    public static <T> T newInnerInstance(Class<? extends T> innerClassToInstantiate, Object outerClassInstance, Object... nonNullArgs) {
        return ConstructorReflection.newInnerInstance(innerClassToInstantiate, outerClassInstance, nonNullArgs);
    }
}
