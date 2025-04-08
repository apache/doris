// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.udf;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.classloader.ScannerLoader;
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.JavaUdfStructType;
import org.apache.doris.common.jni.utils.UdfClassCache;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.thrift.TPrimitiveType;

import com.esotericsoftware.reflectasm.MethodAccess;
import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class BaseExecutor {
    // Object to deserialize ctor params from BE.
    protected static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();
    private static final Logger LOG = Logger.getLogger(BaseExecutor.class);
    protected Object udf;
    // setup by init() and cleared by close()
    protected URLClassLoader classLoader;
    protected UdfClassCache objCache;
    protected TFunction fn;
    protected boolean isStaticLoad = false;
    protected VectorTable outputTable = null;
    String className;

    /**
     * Create a UdfExecutor, using parameters from a serialized thrift object. Used
     * by
     * the backend.
     */

    public BaseExecutor(byte[] thriftParams) throws Exception {
        TJavaUdfExecutorCtorParams request = new TJavaUdfExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        Type[] parameterTypes = new Type[request.fn.arg_types.size()];
        for (int i = 0; i < request.fn.arg_types.size(); ++i) {
            parameterTypes[i] = Type.fromThrift(request.fn.arg_types.get(i));
        }
        fn = request.fn;
        String jarFile = request.location;
        Type funcRetType = Type.fromThrift(request.fn.ret_type);
        if (request.fn.is_udtf_function) {
            funcRetType = ArrayType.create(funcRetType, true);
        }
        init(request, jarFile, funcRetType, parameterTypes);
    }

    public String debugString() {
        StringBuilder res = new StringBuilder();
        for (JavaUdfDataType type : objCache.argTypes) {
            res.append(type.toString());
        }
        res.append(" return type: ").append(objCache.retType.toString());
        res.append(" methodAccess: ").append(objCache.methodAccess.toString());
        res.append(" fn.toString(): ").append(fn.toString());
        return res.toString();
    }

    protected void init(TJavaUdfExecutorCtorParams request, String jarPath,
            Type funcRetType, Type... parameterTypes) throws UdfRuntimeException {
        try {
            isStaticLoad = request.getFn().isSetIsStaticLoad() && request.getFn().is_static_load;
            long expirationTime = 360L; // default is 6 hours
            if (request.getFn().isSetExpirationTime()) {
                expirationTime = request.getFn().getExpirationTime();
            }
            objCache = getClassCache(jarPath, request.getFn().getSignature(), expirationTime,
                    funcRetType, parameterTypes);
            Constructor<?> ctor = objCache.udfClass.getConstructor();
            udf = ctor.newInstance();
        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("Unable to load jar.", e);
        } catch (SecurityException e) {
            throw new UdfRuntimeException("Unable to load function.", e);
        } catch (ClassNotFoundException e) {
            throw new UdfRuntimeException("Unable to find class.", e);
        } catch (NoSuchMethodException e) {
            throw new UdfRuntimeException(
                    "Unable to find constructor with no arguments.", e);
        } catch (IllegalArgumentException e) {
            throw new UdfRuntimeException(
                    "Unable to call UDF constructor with no arguments.", e);
        } catch (Exception e) {
            throw new UdfRuntimeException("Unable to call create UDF instance.", e);
        }
    }


    public UdfClassCache getClassCache(String jarPath, String signature, long expirationTime,
            Type funcRetType, Type... parameterTypes)
            throws MalformedURLException, FileNotFoundException, ClassNotFoundException, InternalException,
            UdfRuntimeException {
        UdfClassCache cache = null;
        if (isStaticLoad) {
            cache = ScannerLoader.getUdfClassLoader(signature);
        }
        if (cache == null) {
            ClassLoader loader;
            if (Strings.isNullOrEmpty(jarPath)) {
                // if jarPath is empty, which means the UDF jar is located in custom_lib
                // and already be loaded when BE start.
                // so here we use system class loader to load UDF class.
                loader = ClassLoader.getSystemClassLoader();
            } else {
                ClassLoader parent = getClass().getClassLoader();
                classLoader = UdfUtils.getClassLoader(jarPath, parent);
                loader = classLoader;
            }
            cache = new UdfClassCache();
            cache.allMethods = new HashMap<>();
            cache.udfClass = Class.forName(className, true, loader);
            cache.methodAccess = MethodAccess.get(cache.udfClass);
            checkAndCacheUdfClass(cache, funcRetType, parameterTypes);
            if (isStaticLoad) {
                ScannerLoader.cacheClassLoader(signature, cache, expirationTime);
            }
        }
        return cache;
    }

    protected abstract void checkAndCacheUdfClass(UdfClassCache cache, Type funcRetType, Type... parameterTypes)
            throws InternalException, UdfRuntimeException;

    /**
     * Close the class loader we may have created.
     */
    public void close() {
        if (classLoader != null) {
            try {
                classLoader.close();
            } catch (IOException e) {
                // Log and ignore.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Error closing the URLClassloader.", e);
                }
            }
        }
        // Close the output table if it exists.
        if (outputTable != null) {
            outputTable.close();
        }
        // We are now un-usable (because the class loader has been
        // closed), so null out method_ and classLoader_.
        classLoader = null;
        objCache.methodAccess = null;
    }

    protected ColumnValueConverter getInputConverter(TPrimitiveType primitiveType, Class clz) {
        switch (primitiveType) {
            case DATE:
            case DATEV2: {
                if (java.util.Date.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new java.util.Date[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                LocalDate v = (LocalDate) columnData[i];
                                result[i] = new java.util.Date(v.getYear() - 1900, v.getMonthValue() - 1,
                                        v.getDayOfMonth());
                            }
                        }
                        return result;
                    };
                } else if (org.joda.time.LocalDate.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new org.joda.time.LocalDate[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                LocalDate v = (LocalDate) columnData[i];
                                result[i] = new org.joda.time.LocalDate(v.getYear(), v.getMonthValue(),
                                        v.getDayOfMonth());
                            }
                        }
                        return result;
                    };
                } else if (!LocalDate.class.equals(clz)) {
                    throw new RuntimeException("Unsupported date type: " + clz.getCanonicalName());
                }
                break;
            }
            case DATETIME:
            case DATETIMEV2: {
                if (org.joda.time.DateTime.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new org.joda.time.DateTime[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                LocalDateTime v = (LocalDateTime) columnData[i];
                                result[i] = new org.joda.time.DateTime(v.getYear(), v.getMonthValue(),
                                        v.getDayOfMonth(), v.getHour(),
                                        v.getMinute(), v.getSecond(), v.getNano() / 1000000);
                            }
                        }
                        return result;
                    };
                } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new org.joda.time.LocalDateTime[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                LocalDateTime v = (LocalDateTime) columnData[i];
                                result[i] = new org.joda.time.LocalDateTime(v.getYear(), v.getMonthValue(),
                                        v.getDayOfMonth(), v.getHour(),
                                        v.getMinute(), v.getSecond(), v.getNano() / 1000000);
                            }
                        }
                        return result;
                    };
                } else if (!LocalDateTime.class.equals(clz)) {
                    throw new RuntimeException("Unsupported date type: " + clz.getCanonicalName());
                }
                break;
            }
            case STRUCT: {
                return (Object[] columnData) -> {
                    Object[] result = new ArrayList[columnData.length];
                    for (int i = 0; i < columnData.length; ++i) {
                        if (columnData[i] != null) {
                            HashMap<String, Object> value = (HashMap<String, Object>) columnData[i];
                            ArrayList<Object> elements = new ArrayList<>();
                            for (Entry<String, Object> entry : value.entrySet()) {
                                elements.add(entry.getValue());
                            }
                            result[i] = elements;
                        }
                    }
                    return result;
                };
            }
            default:
                break;
        }
        return null;
    }

    protected ColumnValueConverter getOutputConverter(JavaUdfDataType returnType, Class clz) {
        switch (returnType.getPrimitiveType()) {
            case DATE:
            case DATEV2: {
                if (java.util.Date.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new LocalDate[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                java.util.Date v = (java.util.Date) columnData[i];
                                result[i] = LocalDate.of(v.getYear() + 1900, v.getMonth() + 1, v.getDate());
                            }
                        }
                        return result;
                    };
                } else if (org.joda.time.LocalDate.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new LocalDate[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                org.joda.time.LocalDate v = (org.joda.time.LocalDate) columnData[i];
                                result[i] = LocalDate.of(v.getYear(), v.getMonthOfYear(), v.getDayOfMonth());
                            }
                        }
                        return result;
                    };
                } else if (!LocalDate.class.equals(clz)) {
                    throw new RuntimeException("Unsupported date type: " + clz.getCanonicalName());
                }
                break;
            }
            case DATETIME:
            case DATETIMEV2: {
                if (org.joda.time.DateTime.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new LocalDateTime[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                org.joda.time.DateTime v = (org.joda.time.DateTime) columnData[i];
                                result[i] = LocalDateTime.of(v.getYear(), v.getMonthOfYear(), v.getDayOfMonth(),
                                        v.getHourOfDay(),
                                        v.getMinuteOfHour(), v.getSecondOfMinute(), v.getMillisOfSecond() * 1000000);
                            }
                        }
                        return result;
                    };
                } else if (org.joda.time.LocalDateTime.class.equals(clz)) {
                    return (Object[] columnData) -> {
                        Object[] result = new LocalDateTime[columnData.length];
                        for (int i = 0; i < columnData.length; ++i) {
                            if (columnData[i] != null) {
                                org.joda.time.LocalDateTime v = (org.joda.time.LocalDateTime) columnData[i];
                                result[i] = LocalDateTime.of(v.getYear(), v.getMonthOfYear(), v.getDayOfMonth(),
                                        v.getHourOfDay(),
                                        v.getMinuteOfHour(), v.getSecondOfMinute(), v.getMillisOfSecond() * 1000000);
                            }
                        }
                        return result;
                    };
                } else if (!LocalDateTime.class.equals(clz)) {
                    throw new RuntimeException("Unsupported date type: " + clz.getCanonicalName());
                }
                break;
            }
            case STRUCT: {
                return (Object[] columnData) -> {
                    Object[] result = (HashMap<String, Object>[]) new HashMap<?, ?>[columnData.length];
                    ArrayList<String> names = ((JavaUdfStructType) returnType).getFieldNames();
                    for (int i = 0; i < columnData.length; ++i) {
                        HashMap<String, Object> elements = new HashMap<String, Object>();
                        if (columnData[i] != null) {
                            ArrayList<Object> v = (ArrayList<Object>) columnData[i];
                            for (int k = 0; k < v.size(); ++k) {
                                elements.put(names.get(k), v.get(k));
                            }
                            result[i] = elements;
                        }
                    }
                    return result;
                };
            }
            default:
                break;
        }
        return null;
    }

    // Add unified converter methods
    protected Map<Integer, ColumnValueConverter> getInputConverters(int numColumns, boolean isUdaf) {
        Map<Integer, ColumnValueConverter> converters = new HashMap<>();
        for (int j = 0; j < numColumns; ++j) {
            // For UDAF, we need to offset by 1 since first arg is state
            int argIndex = isUdaf ? j + 1 : j;
            ColumnValueConverter converter = getInputConverter(objCache.argTypes[j].getPrimitiveType(),
                    objCache.argClass[argIndex]);
            if (converter != null) {
                converters.put(j, converter);
            }
        }
        return converters;
    }

    protected ColumnValueConverter getOutputConverter() {
        return getOutputConverter(objCache.retType, objCache.retClass);
    }
}
