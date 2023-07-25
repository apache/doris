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
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.utils.UdfUtils.JavaUdfDataType;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.esotericsoftware.reflectasm.MethodAccess;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

public class UdfExecutor extends BaseExecutor {
    // private static final java.util.logging.Logger LOG =
    // Logger.getLogger(UdfExecutor.class);
    public static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    // setup by init() and cleared by close()
    private Method method;

    // Pre-constructed input objects for the UDF. This minimizes object creation
    // overhead
    // as these objects are reused across calls to evaluate().
    private Object[] inputObjects;

    private long outputOffset;
    private long rowIdx;

    private long batchSizePtr;
    private int evaluateIndex;
    private MethodAccess methodAccess;

    /**
     * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
     * the backend.
     */
    public UdfExecutor(byte[] thriftParams) throws Exception {
        super(thriftParams);
    }

    /**
     * Close the class loader we may have created.
     */
    @Override
    public void close() {
        // We are now un-usable (because the class loader has been
        // closed), so null out method_ and classLoader_.
        method = null;
        super.close();
    }

    /**
     * evaluate function called by the backend. The inputs to the UDF have
     * been serialized to 'input'
     */
    public void evaluate() throws UdfRuntimeException {
        int batchSize = UdfUtils.UNSAFE.getInt(null, batchSizePtr);
        try {
            if (retType.equals(JavaUdfDataType.STRING) || retType.equals(JavaUdfDataType.VARCHAR)
                    || retType.equals(JavaUdfDataType.CHAR) || retType.equals(JavaUdfDataType.ARRAY_TYPE)) {
                // If this udf return variable-size type (e.g.) String, we have to allocate output
                // buffer multiple times until buffer size is enough to store output column. So we
                // always begin with the last evaluated row instead of beginning of this batch.
                rowIdx = UdfUtils.UNSAFE.getLong(null, outputIntermediateStatePtr + 8);
                if (rowIdx == 0) {
                    outputOffset = 0L;
                }
            } else {
                rowIdx = 0;
            }
            for (; rowIdx < batchSize; rowIdx++) {
                inputObjects = allocateInputObjects(rowIdx, 0);
                // `storeUdfResult` is called to store udf result to output column. If true
                // is returned, current value is stored successfully. Otherwise, current result is
                // not processed successfully (e.g. current output buffer is not large enough) so
                // we break this loop directly.
                if (!storeUdfResult(evaluate(inputObjects), rowIdx, method.getReturnType())) {
                    UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, rowIdx);
                    return;
                }
            }
        } catch (Exception e) {
            if (retType.equals(JavaUdfDataType.STRING) || retType.equals(JavaUdfDataType.ARRAY_TYPE)) {
                UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, batchSize);
            }
            throw new UdfRuntimeException("UDF::evaluate() ran into a problem.", e);
        }
        if (retType.equals(JavaUdfDataType.STRING) || retType.equals(JavaUdfDataType.ARRAY_TYPE)) {
            UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, rowIdx);
        }
    }

    public Object[] convertBasicArguments(int argIdx, boolean isNullable, int numRows, long nullMapAddr,
            long columnAddr, long strOffsetAddr) {
        return convertBasicArg(true, argIdx, isNullable, 0, numRows, nullMapAddr, columnAddr, strOffsetAddr);
    }

    public Object[] convertArrayArguments(int argIdx, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        return convertArrayArg(argIdx, isNullable, 0, numRows, nullMapAddr, offsetsAddr, nestedNullMapAddr, dataAddr,
                strOffsetAddr);
    }

    public Object[] convertMapArguments(int argIdx, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long keyNestedNullMapAddr, long keyDataAddr, long keyStrOffsetAddr,
            long valueNestedNullMapAddr, long valueDataAddr, long valueStrOffsetAddr) {
        PrimitiveType keyType = argTypes[argIdx].getKeyType().getPrimitiveType();
        PrimitiveType valueType = argTypes[argIdx].getValueType().getPrimitiveType();
        Object[] keyCol = convertMapArg(keyType, argIdx, isNullable, 0, numRows, nullMapAddr, offsetsAddr,
                keyNestedNullMapAddr, keyDataAddr,
                keyStrOffsetAddr);
        Object[] valueCol = convertMapArg(valueType, argIdx, isNullable, 0, numRows, nullMapAddr, offsetsAddr,
                valueNestedNullMapAddr, valueDataAddr,
                valueStrOffsetAddr);
        switch (keyType) {
            case BOOLEAN: {
                return new HashMapBuilder<Boolean>().get(keyCol, valueCol, valueType);
            }
            case TINYINT: {
                return new HashMapBuilder<Byte>().get(keyCol, valueCol, valueType);
            }
            case SMALLINT: {
                return new HashMapBuilder<Short>().get(keyCol, valueCol, valueType);
            }
            case INT: {
                return new HashMapBuilder<Integer>().get(keyCol, valueCol, valueType);
            }
            case BIGINT: {
                return new HashMapBuilder<Long>().get(keyCol, valueCol, valueType);
            }
            case LARGEINT: {
                return new HashMapBuilder<BigInteger>().get(keyCol, valueCol, valueType);
            }
            case FLOAT: {
                return new HashMapBuilder<Float>().get(keyCol, valueCol, valueType);
            }
            case DOUBLE: {
                return new HashMapBuilder<Double>().get(keyCol, valueCol, valueType);
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                return new HashMapBuilder<String>().get(keyCol, valueCol, valueType);
            }
            case DATEV2:
            case DATE: {
                return new HashMapBuilder<LocalDate>().get(keyCol, valueCol, valueType);
            }
            case DATETIMEV2:
            case DATETIME: {
                return new HashMapBuilder<LocalDateTime>().get(keyCol, valueCol, valueType);
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMALV2:
            case DECIMAL128: {
                return new HashMapBuilder<BigDecimal>().get(keyCol, valueCol, valueType);
            }
            default: {
                LOG.info("Not support: " + keyType);
                Preconditions.checkState(false, "Not support type " + keyType.toString());
                break;
            }
        }
        return null;
    }

    /**
     * Evaluates the UDF with 'args' as the input to the UDF.
     */
    public Object[] evaluate(int numRows, Object[] column) throws UdfRuntimeException {
        try {
            Object[] result = (Object[]) Array.newInstance(method.getReturnType(), numRows);
            Object[][] inputs = (Object[][]) column;
            Object[] parameters = new Object[inputs.length];
            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < column.length; ++j) {
                    parameters[j] = inputs[j][i];
                }
                result[i] = methodAccess.invoke(udf, evaluateIndex, parameters);
            }
            return result;
        } catch (Exception e) {
            LOG.info("evaluate(int numRows, Object[] column) Exception: " + e.toString());
            throw new UdfRuntimeException("UDF failed to evaluate", e);
        }
    }

    public void copyBatchBasicResult(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long resColumnAddr, long strOffsetAddr) {
        switch (retType) {
            case BOOLEAN: {
                UdfConvert.copyBatchBooleanResult(isNullable, numRows, (Boolean[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case TINYINT: {
                UdfConvert.copyBatchTinyIntResult(isNullable, numRows, (Byte[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case SMALLINT: {
                UdfConvert.copyBatchSmallIntResult(isNullable, numRows, (Short[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case INT: {
                UdfConvert.copyBatchIntResult(isNullable, numRows, (Integer[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case BIGINT: {
                UdfConvert.copyBatchBigIntResult(isNullable, numRows, (Long[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case LARGEINT: {
                UdfConvert.copyBatchLargeIntResult(isNullable, numRows, (BigInteger[]) result, nullMapAddr,
                        resColumnAddr);
                break;
            }
            case FLOAT: {
                UdfConvert.copyBatchFloatResult(isNullable, numRows, (Float[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case DOUBLE: {
                UdfConvert.copyBatchDoubleResult(isNullable, numRows, (Double[]) result, nullMapAddr, resColumnAddr);
                break;
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                UdfConvert.copyBatchStringResult(isNullable, numRows, (String[]) result, nullMapAddr, resColumnAddr,
                        strOffsetAddr);
                break;
            }
            case DATE: {
                UdfConvert.copyBatchDateResult(method.getReturnType(), isNullable, numRows, result,
                        nullMapAddr, resColumnAddr);
                break;
            }
            case DATETIME: {
                UdfConvert
                        .copyBatchDateTimeResult(method.getReturnType(), isNullable, numRows, result,
                                nullMapAddr,
                                resColumnAddr);
                break;
            }
            case DATEV2: {
                UdfConvert.copyBatchDateV2Result(method.getReturnType(), isNullable, numRows, result,
                        nullMapAddr,
                        resColumnAddr);
                break;
            }
            case DATETIMEV2: {
                UdfConvert.copyBatchDateTimeV2Result(method.getReturnType(), isNullable, numRows,
                        result, nullMapAddr,
                        resColumnAddr);
                break;
            }
            case DECIMALV2:
            case DECIMAL128: {
                UdfConvert.copyBatchDecimal128Result(retType.getScale(), isNullable, numRows, (BigDecimal[]) result,
                        nullMapAddr,
                        resColumnAddr);
                break;
            }
            case DECIMAL32: {
                UdfConvert.copyBatchDecimal32Result(retType.getScale(), isNullable, numRows, (BigDecimal[]) result,
                        nullMapAddr,
                        resColumnAddr);
                break;
            }
            case DECIMAL64: {
                UdfConvert.copyBatchDecimal64Result(retType.getScale(), isNullable, numRows, (BigDecimal[]) result,
                        nullMapAddr,
                        resColumnAddr);
                break;
            }
            default: {
                LOG.info("Not support return type: " + retType);
                Preconditions.checkState(false, "Not support type: " + retType.toString());
                break;
            }
        }
    }


    public void copyBatchArrayResult(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        Preconditions.checkState(result.length == numRows,
                "copyBatchArrayResult result size should equal;");
        long hasPutElementNum = 0;
        for (int row = 0; row < numRows; ++row) {
            switch (retType.getItemType().getPrimitiveType()) {
                case BOOLEAN: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayBooleanResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case TINYINT: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayTinyIntResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case SMALLINT: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArraySmallIntResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case INT: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayIntResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case BIGINT: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayBigIntResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case LARGEINT: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayLargeIntResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case FLOAT: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayFloatResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DOUBLE: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDoubleResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayStringResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr, strOffsetAddr);
                    break;
                }
                case DATE: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDateResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIME: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDateTimeResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATEV2: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDateV2Result(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIMEV2: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDateTimeV2Result(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMALV2: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDecimalResult(hasPutElementNum, isNullable, row, result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL32: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDecimalV3Result(retType.getScale(), 4L, hasPutElementNum, isNullable, row,
                                    result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL64: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDecimalV3Result(retType.getScale(), 8L, hasPutElementNum, isNullable, row,
                                    result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL128: {
                    hasPutElementNum = UdfConvert
                            .copyBatchArrayDecimalV3Result(retType.getScale(), 16L, hasPutElementNum, isNullable, row,
                                    result, nullMapAddr,
                                    offsetsAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                default: {
                    Preconditions.checkState(false, "Not support type in array: " + retType);
                    break;
                }
            }
        }
    }

    /**
     * Evaluates the UDF with 'args' as the input to the UDF.
     */
    private Object evaluate(Object... args) throws UdfRuntimeException {
        try {
            return method.invoke(udf, args);
        } catch (Exception e) {
            throw new UdfRuntimeException("UDF failed to evaluate", e);
        }
    }

    public Method getMethod() {
        return method;
    }

    // Sets the result object 'obj' into the outputBufferPtr and outputNullPtr_
    @Override
    protected boolean storeUdfResult(Object obj, long row, Class retClass) throws UdfRuntimeException {
        if (obj == null) {
            if (UdfUtils.UNSAFE.getLong(null, outputNullPtr) == -1) {
                throw new UdfRuntimeException("UDF failed to store null data to not null column");
            }
            UdfUtils.UNSAFE.putByte(null, UdfUtils.UNSAFE.getLong(null, outputNullPtr) + row, (byte) 1);
            if (retType.equals(JavaUdfDataType.STRING)) {
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr)
                        + 4L * row, Integer.parseUnsignedInt(String.valueOf(outputOffset)));
            } else if (retType.equals(JavaUdfDataType.ARRAY_TYPE)) {
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(outputOffset)));
            }
            return true;
        }
        return super.storeUdfResult(obj, row, retClass);
    }

    @Override
    protected long getCurrentOutputOffset(long row, boolean isArrayType) {
        return outputOffset;
    }

    @Override
    protected void updateOutputOffset(long offset) {
        outputOffset = offset;
    }

    // Preallocate the input objects that will be passed to the underlying UDF.
    // These objects are allocated once and reused across calls to evaluate()
    @Override
    protected void init(TJavaUdfExecutorCtorParams request, String jarPath, Type funcRetType,
            Type... parameterTypes) throws UdfRuntimeException {
        String className = request.fn.scalar_fn.symbol;
        batchSizePtr = request.batch_size_ptr;
        outputOffset = 0L;
        rowIdx = 0L;
        ArrayList<String> signatures = Lists.newArrayList();
        try {
            LOG.debug("Loading UDF '" + className + "' from " + jarPath);
            ClassLoader loader;
            if (jarPath != null) {
                // Save for cleanup.
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
            Method[] methods = c.getMethods();
            for (Method m : methods) {
                // By convention, the udf must contain the function "evaluate"
                if (!m.getName().equals(UDF_FUNCTION_NAME)) {
                    continue;
                }
                signatures.add(m.toGenericString());
                argClass = m.getParameterTypes();

                // Try to match the arguments
                if (argClass.length != parameterTypes.length) {
                    continue;
                }
                method = m;
                evaluateIndex = methodAccess.getIndex(UDF_FUNCTION_NAME);
                Pair<Boolean, JavaUdfDataType> returnType;
                if (argClass.length == 0 && parameterTypes.length == 0) {
                    // Special case where the UDF doesn't take any input args
                    returnType = UdfUtils.setReturnType(funcRetType, m.getReturnType());
                    if (!returnType.first) {
                        continue;
                    } else {
                        retType = returnType.second;
                    }
                    argTypes = new JavaUdfDataType[0];
                    LOG.debug("Loaded UDF '" + className + "' from " + jarPath);
                    return;
                }
                returnType = UdfUtils.setReturnType(funcRetType, m.getReturnType());
                if (!returnType.first) {
                    continue;
                } else {
                    retType = returnType.second;
                }
                Pair<Boolean, JavaUdfDataType[]> inputType = UdfUtils.setArgTypes(parameterTypes, argClass, false);
                if (!inputType.first) {
                    continue;
                } else {
                    argTypes = inputType.second;
                }
                LOG.debug("Loaded UDF '" + className + "' from " + jarPath);
                return;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find evaluate function with the correct signature: ")
                    .append(className + ".evaluate(")
                    .append(Joiner.on(", ").join(parameterTypes))
                    .append(")\n")
                    .append("UDF contains: \n    ")
                    .append(Joiner.on("\n    ").join(signatures));
            throw new UdfRuntimeException(sb.toString());
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

    public static class HashMapBuilder<keyType> {
        public Object[] get(Object[] keyCol, Object[] valueCol, PrimitiveType valueType) {
            switch (valueType) {
                case BOOLEAN: {
                    return new BuildMapFromType<keyType, Boolean>().get(keyCol, valueCol);
                }
                case TINYINT: {
                    return new BuildMapFromType<keyType, Byte>().get(keyCol, valueCol);
                }
                case SMALLINT: {
                    return new BuildMapFromType<keyType, Short>().get(keyCol, valueCol);
                }
                case INT: {
                    return new BuildMapFromType<keyType, Integer>().get(keyCol, valueCol);
                }
                case BIGINT: {
                    return new BuildMapFromType<keyType, Long>().get(keyCol, valueCol);
                }
                case LARGEINT: {
                    return new BuildMapFromType<keyType, BigInteger>().get(keyCol, valueCol);
                }
                case FLOAT: {
                    return new BuildMapFromType<keyType, Float>().get(keyCol, valueCol);
                }
                case DOUBLE: {
                    return new BuildMapFromType<keyType, Double>().get(keyCol, valueCol);
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    return new BuildMapFromType<keyType, String>().get(keyCol, valueCol);
                }
                case DATEV2:
                case DATE: {
                    return new BuildMapFromType<keyType, LocalDate>().get(keyCol, valueCol);
                }
                case DATETIMEV2:
                case DATETIME: {
                    return new BuildMapFromType<keyType, LocalDateTime>().get(keyCol, valueCol);
                }
                case DECIMAL32:
                case DECIMAL64:
                case DECIMALV2:
                case DECIMAL128: {
                    return new BuildMapFromType<keyType, BigDecimal>().get(keyCol, valueCol);
                }
                default: {
                    LOG.info("Not support: " + valueType);
                    Preconditions.checkState(false, "Not support type " + valueType.toString());
                    break;
                }
            }
            return null;
        }
    }

    public static class BuildMapFromType<T1, T2> {
        public Object[] get(Object[] keyCol, Object[] valueCol) {
            Object[] retHashMap = new HashMap[keyCol.length];
            for (int colIdx = 0; colIdx < keyCol.length; colIdx++) {
                HashMap<T1, T2> hashMap = new HashMap<>();
                ArrayList<T1> keys = (ArrayList<T1>) (keyCol[colIdx]);
                ArrayList<T2> values = (ArrayList<T2>) (valueCol[colIdx]);
                for (int i = 0; i < keys.size(); i++) {
                    T1 key = keys.get(i);
                    T2 value = values.get(i);
                    if (!hashMap.containsKey(key)) {
                        hashMap.put(key, value);
                    }
                }
                retHashMap[colIdx] = hashMap;
            }
            return retHashMap;
        }
    }
}
