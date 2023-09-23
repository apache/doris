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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.esotericsoftware.reflectasm.MethodAccess;
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
import java.util.Arrays;
import java.util.HashMap;

/**
 * udaf executor.
 */
public class UdafExecutor extends BaseExecutor {

    private static final Logger LOG = Logger.getLogger(UdafExecutor.class);

    private HashMap<String, Method> allMethods;
    private HashMap<Long, Object> stateObjMap;
    private Class retClass;
    private int addIndex;

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

    public Object[] convertBasicArguments(int argIdx, boolean isNullable, int rowStart, int rowEnd, long nullMapAddr,
            long columnAddr, long strOffsetAddr) {
        return convertBasicArg(false, argIdx, isNullable, rowStart, rowEnd, nullMapAddr, columnAddr, strOffsetAddr);
    }

    public Object[] convertArrayArguments(int argIdx, boolean isNullable, int rowStart, int rowEnd, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        return convertArrayArg(argIdx, isNullable, rowStart, rowEnd, nullMapAddr, offsetsAddr, nestedNullMapAddr,
                dataAddr, strOffsetAddr);
    }

    public Object[] convertMapArguments(int argIdx, boolean isNullable, int rowStart, int rowEnd, long nullMapAddr,
            long offsetsAddr, long keyNestedNullMapAddr, long keyDataAddr, long keyStrOffsetAddr,
            long valueNestedNullMapAddr, long valueDataAddr, long valueStrOffsetAddr) {
        PrimitiveType keyType = argTypes[argIdx].getKeyType().getPrimitiveType();
        PrimitiveType valueType = argTypes[argIdx].getValueType().getPrimitiveType();
        Object[] keyCol = convertMapArg(keyType, argIdx, isNullable, rowStart, rowEnd, nullMapAddr, offsetsAddr,
                keyNestedNullMapAddr, keyDataAddr,
                keyStrOffsetAddr, argTypes[argIdx].getKeyScale());
        Object[] valueCol = convertMapArg(valueType, argIdx, isNullable, rowStart, rowEnd, nullMapAddr, offsetsAddr,
                valueNestedNullMapAddr,
                valueDataAddr,
                valueStrOffsetAddr, argTypes[argIdx].getValueScale());
        return buildHashMap(keyType, valueType, keyCol, valueCol);
    }

    public void addBatch(boolean isSinglePlace, int rowStart, int rowEnd, long placeAddr, int offset, Object[] column)
            throws UdfRuntimeException {
        if (isSinglePlace) {
            addBatchSingle(rowStart, rowEnd, placeAddr, column);
        } else {
            addBatchPlaces(rowStart, rowEnd, placeAddr, offset, column);
        }
    }

    public void addBatchSingle(int rowStart, int rowEnd, long placeAddr, Object[] column) throws UdfRuntimeException {
        try {
            Long curPlace = placeAddr;
            Object[] inputArgs = new Object[argTypes.length + 1];
            Object state = stateObjMap.get(curPlace);
            if (state != null) {
                inputArgs[0] = state;
            } else {
                Object newState = createAggState();
                stateObjMap.put(curPlace, newState);
                inputArgs[0] = newState;
            }

            Object[][] inputs = (Object[][]) column;
            for (int i = 0; i < (rowEnd - rowStart); ++i) {
                for (int j = 0; j < column.length; ++j) {
                    inputArgs[j + 1] = inputs[j][i];
                }
                methodAccess.invoke(udf, addIndex, inputArgs);
            }
        } catch (Exception e) {
            LOG.info("evaluate exception debug: " + debugString());
            LOG.info("invoke add function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to addBatchSingle: ", e);
        }
    }

    public void addBatchPlaces(int rowStart, int rowEnd, long placeAddr, int offset, Object[] column)
            throws UdfRuntimeException {
        try {
            Object[][] inputs = (Object[][]) column;
            ArrayList<Object> placeState = new ArrayList<>(rowEnd - rowStart);
            for (int row = rowStart; row < rowEnd; ++row) {
                Long curPlace = UdfUtils.UNSAFE.getLong(null, placeAddr + (8L * row)) + offset;
                Object state = stateObjMap.get(curPlace);
                if (state != null) {
                    placeState.add(state);
                } else {
                    Object newState = createAggState();
                    stateObjMap.put(curPlace, newState);
                    placeState.add(newState);
                }
            }
            //spilt into two for loop

            Object[] inputArgs = new Object[argTypes.length + 1];
            for (int row = 0; row < (rowEnd - rowStart); ++row) {
                inputArgs[0] = placeState.get(row);
                for (int j = 0; j < column.length; ++j) {
                    inputArgs[j + 1] = inputs[j][row];
                }
                methodAccess.invoke(udf, addIndex, inputArgs);
            }
        } catch (Exception e) {
            LOG.info("evaluate exception debug: " + debugString());
            LOG.info("invoke add function meet some error: " + Arrays.toString(e.getStackTrace()));
            throw new UdfRuntimeException("UDAF failed to addBatchPlaces: ", e);
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
            LOG.info("evaluate exception debug: " + debugString());
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
            LOG.info("evaluate exception debug: " + debugString());
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
            LOG.info("evaluate exception debug: " + debugString());
            LOG.warn("invoke merge function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to merge: ", e);
        }
    }

    /**
     * invoke getValue to return finally result.
     */

    public Object getValue(long place) throws UdfRuntimeException {
        try {
            if (stateObjMap.get(place) == null) {
                stateObjMap.put(place, createAggState());
            }
            return allMethods.get(UDAF_RESULT_FUNCTION).invoke(udf, stateObjMap.get((Long) place));
        } catch (Exception e) {
            LOG.info("evaluate exception debug: " + debugString());
            LOG.warn("invoke getValue function meet some error: " + e.getCause().toString());
            throw new UdfRuntimeException("UDAF failed to result", e);
        }
    }

    public void copyTupleBasicResult(Object result, int row, long outputNullMapPtr, long outputBufferBase,
            long charsAddress,
            long offsetsAddr) throws UdfRuntimeException {
        if (result == null) {
            // put null obj
            if (outputNullMapPtr == -1) {
                throw new UdfRuntimeException("UDAF failed to store null data to not null column");
            } else {
                UdfUtils.UNSAFE.putByte(outputNullMapPtr + row, (byte) 1);
            }
            return;
        }
        try {
            if (outputNullMapPtr != -1) {
                UdfUtils.UNSAFE.putByte(outputNullMapPtr + row, (byte) 0);
            }
            copyTupleBasicResult(result, row, retClass, outputBufferBase, charsAddress,
                    offsetsAddr, retType);
        } catch (UdfRuntimeException e) {
            LOG.info(e.toString());
        }
    }

    public void copyTupleArrayResult(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) throws UdfRuntimeException {
        if (nullMapAddr > 0) {
            UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 0);
        }
        copyTupleArrayResultImpl(hasPutElementNum, isNullable, row, result, nullMapAddr, offsetsAddr, nestedNullMapAddr,
                dataAddr, strOffsetAddr, retType.getItemType().getPrimitiveType(), retType.getScale());
    }

    public void copyTupleMapResult(long hasPutElementNum, boolean isNullable, int row, Object result, long nullMapAddr,
            long offsetsAddr,
            long keyNsestedNullMapAddr, long keyDataAddr,
            long keyStrOffsetAddr,
            long valueNsestedNullMapAddr, long valueDataAddr, long valueStrOffsetAddr) throws UdfRuntimeException {
        if (nullMapAddr > 0) {
            UdfUtils.UNSAFE.putByte(nullMapAddr + row, (byte) 0);
        }
        PrimitiveType keyType = retType.getKeyType().getPrimitiveType();
        PrimitiveType valueType = retType.getValueType().getPrimitiveType();
        Object[] keyCol = new Object[1];
        Object[] valueCol = new Object[1];
        Object[] resultArr = new Object[1];
        resultArr[0] = result;
        buildArrayListFromHashMap(resultArr, keyType, valueType, keyCol, valueCol);
        copyTupleArrayResultImpl(hasPutElementNum, isNullable, row,
                valueCol[0], nullMapAddr, offsetsAddr,
                valueNsestedNullMapAddr, valueDataAddr, valueStrOffsetAddr, valueType, retType.getKeyScale());
        copyTupleArrayResultImpl(hasPutElementNum, isNullable, row, keyCol[0], nullMapAddr, offsetsAddr,
                keyNsestedNullMapAddr, keyDataAddr, keyStrOffsetAddr, keyType, retType.getValueScale());
    }

    @Override
    protected void init(TJavaUdfExecutorCtorParams request, String jarPath, Type funcRetType,
            Type... parameterTypes) throws UdfRuntimeException {
        String className = request.fn.aggregate_fn.symbol;
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
            methodAccess = MethodAccess.get(c);
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
                        addIndex = methodAccess.getIndex(UDAF_ADD_FUNCTION);
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
