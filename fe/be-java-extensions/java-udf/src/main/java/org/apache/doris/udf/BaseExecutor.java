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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.thrift.TPrimitiveType;

import com.esotericsoftware.reflectasm.MethodAccess;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.net.URLClassLoader;
import java.time.LocalDate;
import java.time.LocalDateTime;

public abstract class BaseExecutor {
    private static final Logger LOG = Logger.getLogger(BaseExecutor.class);

    // By convention, the function in the class must be called evaluate()
    public static final String UDF_FUNCTION_NAME = "evaluate";
    public static final String UDAF_CREATE_FUNCTION = "create";
    public static final String UDAF_DESTROY_FUNCTION = "destroy";
    public static final String UDAF_ADD_FUNCTION = "add";
    public static final String UDAF_RESET_FUNCTION = "reset";
    public static final String UDAF_SERIALIZE_FUNCTION = "serialize";
    public static final String UDAF_DESERIALIZE_FUNCTION = "deserialize";
    public static final String UDAF_MERGE_FUNCTION = "merge";
    public static final String UDAF_RESULT_FUNCTION = "getValue";

    // Object to deserialize ctor params from BE.
    protected static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();

    protected Object udf;
    // setup by init() and cleared by close()
    protected URLClassLoader classLoader;

    // Return and argument types of the function inferred from the udf method
    // signature.
    // The JavaUdfDataType enum maps it to corresponding primitive type.
    protected JavaUdfDataType[] argTypes;
    protected JavaUdfDataType retType;
    protected Class[] argClass;
    protected MethodAccess methodAccess;
    protected TFunction fn;

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
        init(request, jarFile, funcRetType, parameterTypes);
    }

    public String debugString() {
        StringBuilder res = new StringBuilder();
        for (JavaUdfDataType type : argTypes) {
            res.append(type.toString());
            if (type.getItemType() != null) {
                res.append(" item: ").append(type.getItemType().toString()).append(" sql: ")
                        .append(type.getItemType().toSql());
            }
            if (type.getKeyType() != null) {
                res.append(" key: ").append(type.getKeyType().toString()).append(" sql: ")
                        .append(type.getKeyType().toSql());
            }
            if (type.getValueType() != null) {
                res.append(" key: ").append(type.getValueType().toString()).append(" sql: ")
                        .append(type.getValueType().toSql());
            }
        }
        res.append(" return type: ").append(retType.toString());
        if (retType.getItemType() != null) {
            res.append(" item: ").append(retType.getItemType().toString()).append(" sql: ")
                    .append(retType.getItemType().toSql());
        }
        if (retType.getKeyType() != null) {
            res.append(" key: ").append(retType.getKeyType().toString()).append(" sql: ")
                    .append(retType.getKeyType().toSql());
        }
        if (retType.getValueType() != null) {
            res.append(" key: ").append(retType.getValueType().toString()).append(" sql: ")
                    .append(retType.getValueType().toSql());
        }
        res.append(" methodAccess: ").append(methodAccess.toString());
        res.append(" fn.toString(): ").append(fn.toString());
        return res.toString();
    }

    protected abstract void init(TJavaUdfExecutorCtorParams request, String jarPath,
            Type funcRetType, Type... parameterTypes) throws UdfRuntimeException;

    /**
     * Close the class loader we may have created.
     */
    public void close() {
        if (classLoader != null) {
            try {
                classLoader.close();
            } catch (IOException e) {
                // Log and ignore.
                LOG.debug("Error closing the URLClassloader.", e);
            }
        }
        // We are now un-usable (because the class loader has been
        // closed), so null out method_ and classLoader_.
        classLoader = null;
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
            default:
                break;
        }
        return null;
    }

    protected ColumnValueConverter getOutputConverter(TPrimitiveType primitiveType, Class clz) {
        switch (primitiveType) {
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
            default:
                break;
        }
        return null;
    }
}
