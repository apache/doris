/*
 * Copyright (c) 2006 JMockit developers
 * This file is subject to the terms of the MIT license (see LICENSE.txt).
 */

package org.apache.doris.common.jmockit;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;


/**
 * Modify from mockit.internal.util.FieldReflection JMockit v1.13
 * Util class to set and get the value of specified field.
 */
public final class FieldReflection {
    private FieldReflection() {
    }

    /**
     * Get field's value with field's name.
     */
    public static <T> T getField(Class<?> theClass, String fieldName, Object targetObject) {
        if (theClass == null || fieldName == null || targetObject == null) {
            throw new IllegalStateException();
        }
        Field field = getDeclaredField(theClass, fieldName, targetObject != null);
        return getFieldValue(field, targetObject);
    }

    /**
     * Get field's value with field's type.
     */
    public static <T> T getField(Class<?> theClass, Class<T> fieldType, Object targetObject) {
        if (theClass == null || fieldType == null) {
            throw new IllegalStateException();
        }
        Field field = getDeclaredField(theClass, fieldType, targetObject != null, false);
        return getFieldValue(field, targetObject);
    }

    /**
     * Get field's value with field's type.
     */
    public static <T> T getField(Class<?> theClass, Type fieldType, Object targetObject) {
        if (theClass == null || fieldType == null) {
            throw new IllegalStateException();
        }
        Field field = getDeclaredField(theClass, fieldType, targetObject != null, false);
        return getFieldValue(field, targetObject);
    }

    /**
     * Modify field's value in targetObject.
     * If {@fieldName String} is null, will try to set field with field's type.
     */
    public static Field setField(Class<?> theClass, Object targetObject, String fieldName, Object fieldValue) {
        if (theClass == null) {
            throw new IllegalArgumentException();
        }
        boolean instanceField = targetObject != null;
        Field field;
        if (fieldName != null) {
            field = getDeclaredField(theClass, fieldName, instanceField);
        } else {
            if (fieldValue == null) {
                throw new IllegalArgumentException("Missing field value when setting field by type");
            }

            field = getDeclaredField(theClass, fieldValue.getClass(), instanceField, true);
        }

        setFieldValue(field, targetObject, fieldValue);
        return field;
    }

    /**
     * Get field by field's name.
     * If no field is found in this class, it will continue to look up its super class.
     * If {@instanceField boolean} is true, will only search for the non-static field.
     */
    private static Field getDeclaredField(Class<?> theClass, String fieldName, boolean instanceField) {
        if (theClass == null || fieldName == null) {
            throw new IllegalStateException();
        }
        try {
            return theClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class<?> superClass = theClass.getSuperclass();
            if (superClass != null && superClass != Object.class) {
                return getDeclaredField(superClass, fieldName, instanceField);
            } else {
                String kind = instanceField ? "instance" : "static";
                throw new IllegalArgumentException("No " + kind + " field of name \"" + fieldName + "\" found in " + theClass);
            }
        }
    }

    /**
     * Get field by field's type.
     * If no field is found in this class, it will continue to look up its super class.
     * If {@instanceField boolean} is true, will only search for the non-static field.
     * If {@forAssignment boolean} is true, will compare its super type with desiredType.
     */
    private static Field getDeclaredField(Class<?> theClass, Type desiredType, boolean instanceField, boolean forAssignment) {
        if (theClass == null || desiredType == null) {
            throw new IllegalStateException();
        }
        Field found = getDeclaredFieldInSingleClass(theClass, desiredType, instanceField, forAssignment);
        if (found == null) {
            Class<?> superClass = theClass.getSuperclass();
            if (superClass != null && superClass != Object.class) {
                return getDeclaredField(superClass, desiredType, instanceField, forAssignment);
            } else {
                StringBuilder errorMsg = new StringBuilder(instanceField ? "Instance" : "Static");
                String typeName = getTypeName(desiredType);
                errorMsg.append(" field of type ").append(typeName).append(" not found in ").append(theClass);
                throw new IllegalArgumentException(errorMsg.toString());
            }
        } else {
            return found;
        }
    }

    /**
     * Get field by field's type.
     * There is only one field is expected to be found in a single class.
     * If {@instanceField boolean} is true, will only search for the non-static field.
     * If {@forAssignment boolean} is true, will compare its super type with desiredType.
     * If more than one field are found, a IllegalArgumentException will be thrown.
     */
    private static Field getDeclaredFieldInSingleClass(Class<?> theClass, Type desiredType, boolean instanceField, boolean forAssignment) {
        if (theClass == null || desiredType == null) {
            throw new IllegalStateException();
        }
        Field found = null;
        Field[] fields = theClass.getDeclaredFields();

        for (Field field : fields) {
            if (!field.isSynthetic()) {
                Type fieldType = field.getGenericType();
                if (instanceField != Modifier.isStatic(field.getModifiers()) && isCompatibleFieldType(fieldType, desiredType, forAssignment)) {
                    if (found != null) {
                        String message = errorMessageForMoreThanOneFieldFound(desiredType, instanceField, forAssignment, found, field);
                        throw new IllegalArgumentException(message);
                    }

                    found = field;
                }
            }
        }

        return found;
    }

    /**
     * return true if the {@fieldType} is compatible with {@desiredType}.
     * If {@forAssignment} is true, will compare its super type with desiredType.
     * If {@forAssignment} is false, will also compare it with desiredType's super type.
     */
    private static boolean isCompatibleFieldType(Type fieldType, Type desiredType, boolean forAssignment) {
        if (fieldType == null || desiredType == null) {
            throw new IllegalStateException();
        }
        Class<?> fieldClass = getClassType(fieldType);
        Class<?> desiredClass = getClassType(desiredType);
        if (isSameType(desiredClass, fieldClass)) {
            return true;
        } else if (forAssignment) {
            return fieldClass.isAssignableFrom(desiredClass);
        } else {
            return desiredClass.isAssignableFrom(fieldClass) || fieldClass.isAssignableFrom(desiredClass);
        }
    }

    private static String errorMessageForMoreThanOneFieldFound(Type desiredFieldType, boolean instanceField, boolean forAssignment, Field firstField, Field secondField) {
        return "More than one " + (instanceField ? "instance" : "static") + " field " + (forAssignment ? "to" : "from")
                + " which a value of type "
                + getTypeName(desiredFieldType) + (forAssignment ? " can be assigned" : " can be read") + " exists in "
                + secondField.getDeclaringClass() + ": " + firstField.getName() + ", " + secondField.getName();
    }

    private static String getTypeName(Type type) {
        if (type == null) {
            throw new IllegalStateException();
        }
        Class<?> classType = getClassType(type);
        Class<?> primitiveType = AutoType.getPrimitiveType(classType);
        if (primitiveType != null) {
            return primitiveType + " or " + classType.getSimpleName();
        } else {
            String name = classType.getName();
            return name.startsWith("java.lang.") ? name.substring(10) : name;
        }
    }

    /**
     * Get field in {@targetObject Object}.
     */
    private static <T> T getFieldValue(Field field, Object targetObject) {
        if (field == null) {
            throw new IllegalStateException();
        }
        makeAccessible(field);

        try {
            return (T) field.get(targetObject);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Modify field with value in {@targetObject Object}.
     */
    public static void setFieldValue(Field field, Object targetObject, Object value) {
        if (field == null) {
            throw new IllegalStateException();
        }
        try {
            if (Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())) {
                throw new IllegalArgumentException("Do not allow to set static final field");
            } else {
                makeAccessible(field);
                field.set(targetObject, value);
            }

        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    private static void setStaticFinalField(Field field, Object value) throws IllegalAccessException {
        if (field == null) {
            throw new IllegalStateException();
        }
        Field modifiersField;
        try {
            modifiersField = Field.class.getDeclaredField("modifiers");
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

        modifiersField.setAccessible(true);
        int nonFinalModifiers = modifiersField.getInt(field) - 16;
        modifiersField.setInt(field, nonFinalModifiers);
        FieldAccessor accessor = ReflectionFactory.getReflectionFactory().newFieldAccessor(field, false);
        accessor.set((Object)null, value);
    }
    */

    public static Class<?> getClassType(Type declaredType) {
        while (!(declaredType instanceof Class)) {
            if (declaredType instanceof ParameterizedType) {
                return (Class) ((ParameterizedType) declaredType).getRawType();
            }

            if (!(declaredType instanceof TypeVariable)) {
                throw new IllegalArgumentException("Type of unexpected kind: " + declaredType);
            }

            declaredType = ((TypeVariable) declaredType).getBounds()[0];
        }

        return (Class) declaredType;
    }

    // ensure that field is accessible
    public static void makeAccessible(AccessibleObject classMember) {
        if (!classMember.isAccessible()) {
            classMember.setAccessible(true);
        }
    }

    // return true if the two types are same type.
    private static boolean isSameType(Class<?> firstType, Class<?> secondType) {
        return firstType == secondType
                || firstType.isPrimitive() && firstType == AutoType.getPrimitiveType(secondType)
                || secondType.isPrimitive() && secondType == AutoType.getPrimitiveType(firstType);
    }

}
