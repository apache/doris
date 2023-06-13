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
import org.apache.doris.common.Pair;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.utils.UdfUtils.JavaUdfDataType;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * udaf executor.
 */
public class UdafExecutor extends BaseExecutor {

    private static final Logger LOG = Logger.getLogger(UdafExecutor.class);

    private long inputPlacesPtr;
    private HashMap<String, Method> allMethods;
    private HashMap<Long, Object> stateObjMap;
    private Class retClass;

    /**
     * Constructor to create an object.
     */
    public UdafExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    /**
     * close and invoke destroy function.
     */
    @Override
    public void close() {
        allMethods = null;
        super.close();
    }

    /**
     * invoke add function, add row in loop [rowStart, rowEnd).
     */
    public void add(boolean isSinglePlace, long rowStart, long rowEnd) throws UdfRuntimeException {
        try {
            long idx = rowStart;
            do {
                Long curPlace = null;
                if (isSinglePlace) {
                    curPlace = UdfUtils.UNSAFE.getLong(null, UdfUtils.UNSAFE.getLong(null, inputPlacesPtr));
                } else {
                    curPlace = UdfUtils.UNSAFE.getLong(null, UdfUtils.UNSAFE.getLong(null, inputPlacesPtr) + 8L * idx);
                }
                Object[] inputArgs = new Object[argTypes.length + 1];
                Object state = stateObjMap.get(curPlace);
                if (state != null) {
                    inputArgs[0] = state;
                } else {
                    Object newState = createAggState();
                    stateObjMap.put(curPlace, newState);
                    inputArgs[0] = newState;
                }
                do {
                    Object[] inputObjects = allocateInputObjects(idx, 1);
                    for (int i = 0; i < argTypes.length; ++i) {
                        inputArgs[i + 1] = inputObjects[i];
                    }
                    allMethods.get(UDAF_ADD_FUNCTION).invoke(udf, inputArgs);
                    idx++;
                } while (isSinglePlace && idx < rowEnd);
            } while (idx < rowEnd);
        } catch (Exception e) {
            LOG.warn("invoke add function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to add: ", e);
        }
    }

    /**
     * invoke user create function to get obj.
     */
    public Object createAggState() throws UdfRuntimeException {
        try {
            return allMethods.get(UDAF_CREATE_FUNCTION).invoke(udf, null);
        } catch (Exception e) {
            LOG.warn("invoke createAggState function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to create: ", e);
        }
    }

    /**
     * invoke destroy before colse. Here we destroy all data at once
     */
    public void destroy() throws UdfRuntimeException {
        try {
            for (Object obj : stateObjMap.values()) {
                allMethods.get(UDAF_DESTROY_FUNCTION).invoke(udf, obj);
            }
            stateObjMap.clear();
        } catch (Exception e) {
            LOG.warn("invoke destroy function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to destroy: ", e);
        }
    }

    /**
     * invoke serialize function and return byte[] to backends.
     */
    public byte[] serialize(long place) throws UdfRuntimeException {
        try {
            Object[] args = new Object[2];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            args[0] = stateObjMap.get((Long) place);
            args[1] = new DataOutputStream(baos);
            allMethods.get(UDAF_SERIALIZE_FUNCTION).invoke(udf, args);
            return baos.toByteArray();
        } catch (Exception e) {
            LOG.warn("invoke serialize function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to serialize: ", e);
        }
    }

    /*
     * invoke reset function and reset the state to init.
     */
    public void reset(long place) throws UdfRuntimeException {
        try {
            Object[] args = new Object[1];
            args[0] = stateObjMap.get((Long) place);
            if (args[0] == null) {
                return;
            }
            allMethods.get(UDAF_RESET_FUNCTION).invoke(udf, args);
        } catch (Exception e) {
            LOG.warn("invoke reset function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to reset: ", e);
        }
    }

    /**
     * invoke merge function and it's have done deserialze.
     * here call deserialize first, and call merge.
     */
    public void merge(long place, byte[] data) throws UdfRuntimeException {
        try {
            Object[] args = new Object[2];
            ByteArrayInputStream bins = new ByteArrayInputStream(data);
            args[0] = createAggState();
            args[1] = new DataInputStream(bins);
            allMethods.get(UDAF_DESERIALIZE_FUNCTION).invoke(udf, args);
            args[1] = args[0];
            Long curPlace = place;
            Object state = stateObjMap.get(curPlace);
            if (state != null) {
                args[0] = state;
            } else {
                Object newState = createAggState();
                stateObjMap.put(curPlace, newState);
                args[0] = newState;
            }
            allMethods.get(UDAF_MERGE_FUNCTION).invoke(udf, args);
        } catch (Exception e) {
            LOG.warn("invoke merge function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to merge: ", e);
        }
    }

    /**
     * invoke getValue to return finally result.
     */
    public boolean getValue(long row, long place) throws UdfRuntimeException {
        try {
            if (stateObjMap.get(place) == null) {
                stateObjMap.put(place, createAggState());
            }
            return storeUdfResult(allMethods.get(UDAF_RESULT_FUNCTION).invoke(udf, stateObjMap.get((Long) place)),
                    row, retClass);
        } catch (Exception e) {
            LOG.warn("invoke getValue function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to result", e);
        }
    }

    @Override
    protected boolean storeUdfResult(Object obj, long row, Class retClass) throws UdfRuntimeException {
        if (obj == null) {
            // If result is null, return true directly when row == 0 as we have already inserted default value.
            if (UdfUtils.UNSAFE.getLong(null, outputNullPtr) == -1) {
                throw new UdfRuntimeException("UDAF failed to store null data to not null column");
            }
            return true;
        }
        return super.storeUdfResult(obj, row, retClass);
    }

    @Override
    protected long getCurrentOutputOffset(long row, boolean isArrayType) {
        if (isArrayType) {
            return Integer.toUnsignedLong(
                UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * (row - 1)));
        } else {
            return Integer.toUnsignedLong(
                UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 4L * (row - 1)));
        }
    }

    @Override
    protected void init(TJavaUdfExecutorCtorParams request, String jarPath, Type funcRetType,
            Type... parameterTypes) throws UdfRuntimeException {
        String className = request.fn.aggregate_fn.symbol;
        inputPlacesPtr = request.input_places_ptr;
        allMethods = new HashMap<>();
        stateObjMap = new HashMap<>();

        ArrayList<String> signatures = Lists.newArrayList();
        try {
            ClassLoader loader;
            if (jarPath != null) {
                ClassLoader parent = getClass().getClassLoader();
                classLoader = UdfUtils.getClassLoader(jarPath, parent);
                loader = classLoader;
            } else {
                // for test
                loader = ClassLoader.getSystemClassLoader();
            }
            Class<?> c = Class.forName(className, true, loader);
            Constructor<?> ctor = c.getConstructor();
            udf = ctor.newInstance();
            Method[] methods = c.getDeclaredMethods();
            int idx = 0;
            for (idx = 0; idx < methods.length; ++idx) {
                signatures.add(methods[idx].toGenericString());
                switch (methods[idx].getName()) {
                    case UDAF_DESTROY_FUNCTION:
                    case UDAF_CREATE_FUNCTION:
                    case UDAF_MERGE_FUNCTION:
                    case UDAF_SERIALIZE_FUNCTION:
                    case UDAF_RESET_FUNCTION:
                    case UDAF_DESERIALIZE_FUNCTION: {
                        allMethods.put(methods[idx].getName(), methods[idx]);
                        break;
                    }
                    case UDAF_RESULT_FUNCTION: {
                        allMethods.put(methods[idx].getName(), methods[idx]);
                        Pair<Boolean, JavaUdfDataType> returnType = UdfUtils.setReturnType(funcRetType,
                                methods[idx].getReturnType());
                        if (!returnType.first) {
                            LOG.debug("result function set return parameterTypes has error");
                        } else {
                            retType = returnType.second;
                            retClass = methods[idx].getReturnType();
                        }
                        break;
                    }
                    case UDAF_ADD_FUNCTION: {
                        allMethods.put(methods[idx].getName(), methods[idx]);

                        argClass = methods[idx].getParameterTypes();
                        if (argClass.length != parameterTypes.length + 1) {
                            LOG.debug("add function parameterTypes length not equal " + argClass.length + " "
                                    + parameterTypes.length + " " + methods[idx].getName());
                        }
                        if (!(parameterTypes.length == 0)) {
                            Pair<Boolean, JavaUdfDataType[]> inputType = UdfUtils.setArgTypes(parameterTypes,
                                    argClass, true);
                            if (!inputType.first) {
                                LOG.debug("add function set arg parameterTypes has error");
                            } else {
                                argTypes = inputType.second;
                            }
                        } else {
                            // Special case where the UDF doesn't take any input args
                            argTypes = new JavaUdfDataType[0];
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
            if (idx == methods.length) {
                return;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find evaluate function with the correct signature: ").append(className + ".evaluate(")
                    .append(Joiner.on(", ").join(parameterTypes)).append(")\n").append("UDF contains: \n    ")
                    .append(Joiner.on("\n    ").join(signatures));
            throw new UdfRuntimeException(sb.toString());

        } catch (MalformedURLException e) {
            throw new UdfRuntimeException("Unable to load jar.", e);
        } catch (SecurityException e) {
            throw new UdfRuntimeException("Unable to load function.", e);
        } catch (ClassNotFoundException e) {
            throw new UdfRuntimeException("Unable to find class.", e);
        } catch (NoSuchMethodException e) {
            throw new UdfRuntimeException("Unable to find constructor with no arguments.", e);
        } catch (IllegalArgumentException e) {
            throw new UdfRuntimeException("Unable to call UDAF constructor with no arguments.", e);
        } catch (Exception e) {
            throw new UdfRuntimeException("Unable to call create UDAF instance.", e);
        }
    }
}
