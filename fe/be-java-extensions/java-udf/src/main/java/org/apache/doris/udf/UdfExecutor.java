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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.util.ArrayList;

public class UdfExecutor extends BaseExecutor {
    private static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    // setup by init() and cleared by close()
    private Method method;

    // Pre-constructed input objects for the UDF. This minimizes object creation overhead
    // as these objects are reused across calls to evaluate().
    private Object[] inputObjects;

    private long outputOffset;
    private long rowIdx;

    private long batchSizePtr;

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
        switch (argTypes[argIdx]) {
            case BOOLEAN:
                return UdfConvert.convertBooleanArg(isNullable, numRows, nullMapAddr, columnAddr);
            case TINYINT:
                return UdfConvert.convertTinyIntArg(isNullable, numRows, nullMapAddr, columnAddr);
            case SMALLINT:
                return UdfConvert.convertSmallIntArg(isNullable, numRows, nullMapAddr, columnAddr);
            case INT:
                return UdfConvert.convertIntArg(isNullable, numRows, nullMapAddr, columnAddr);
            case BIGINT:
                return UdfConvert.convertBigIntArg(isNullable, numRows, nullMapAddr, columnAddr);
            case LARGEINT:
                return UdfConvert.convertLargeIntArg(isNullable, numRows, nullMapAddr, columnAddr);
            case FLOAT:
                return UdfConvert.convertFloatArg(isNullable, numRows, nullMapAddr, columnAddr);
            case DOUBLE:
                return UdfConvert.convertDoubleArg(isNullable, numRows, nullMapAddr, columnAddr);
            case CHAR:
            case VARCHAR:
            case STRING:
                return UdfConvert.convertStringArg(isNullable, numRows, nullMapAddr, columnAddr, strOffsetAddr);
            case DATE: // udaf maybe argClass[i + argClassOffset] need add +1
                return UdfConvert.convertDateArg(argClass[argIdx], isNullable, numRows, nullMapAddr, columnAddr);
            case DATETIME:
                return UdfConvert.convertDateTimeArg(argClass[argIdx], isNullable, numRows, nullMapAddr, columnAddr);
            case DATEV2:
                return UdfConvert.convertDateV2Arg(argClass[argIdx], isNullable, numRows, nullMapAddr, columnAddr);
            case DATETIMEV2:
                return UdfConvert.convertDateTimeV2Arg(argClass[argIdx], isNullable, numRows, nullMapAddr, columnAddr);
            case DECIMALV2:
            case DECIMAL128:
                return UdfConvert.convertDecimalArg(argTypes[argIdx].getScale(), 16L, isNullable, numRows, nullMapAddr,
                        columnAddr);
            case DECIMAL32:
                return UdfConvert.convertDecimalArg(argTypes[argIdx].getScale(), 4L, isNullable, numRows, nullMapAddr,
                        columnAddr);
            case DECIMAL64:
                return UdfConvert.convertDecimalArg(argTypes[argIdx].getScale(), 8L, isNullable, numRows, nullMapAddr,
                        columnAddr);
            default: {
                LOG.info("Not support type: " + argTypes[argIdx].toString());
                Preconditions.checkState(false, "Not support type: " + argTypes[argIdx].toString());
                break;
            }
        }
        return null;
    }


    public Object[] convertArrayArguments(int argIdx, boolean isNullable, int numRows, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        Object[] argument = (Object[]) Array.newInstance(ArrayList.class, numRows);
        for (int row = 0; row < numRows; ++row) {
            long offsetStart = UdfUtils.UNSAFE.getLong(null, offsetsAddr + 8L * (row - 1));
            long offsetEnd = UdfUtils.UNSAFE.getLong(null, offsetsAddr + 8L * (row));
            int currentRowNum = (int) (offsetEnd - offsetStart);
            switch (argTypes[argIdx].getItemType().getPrimitiveType()) {
                case BOOLEAN: {
                    UdfConvert
                            .convertArrayBooleanArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case TINYINT: {
                    UdfConvert
                            .convertArrayTinyIntArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case SMALLINT: {
                    UdfConvert
                            .convertArraySmallIntArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case INT: {
                    UdfConvert.convertArrayIntArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case BIGINT: {
                    UdfConvert.convertArrayBigIntArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case LARGEINT: {
                    UdfConvert
                            .convertArrayLargeIntArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case FLOAT: {
                    UdfConvert.convertArrayFloatArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case DOUBLE: {
                    UdfConvert.convertArrayDoubleArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    UdfConvert.convertArrayStringArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr, strOffsetAddr);
                    break;
                }
                case DATE: {
                    UdfConvert.convertArrayDateArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIME: {
                    UdfConvert
                            .convertArrayDateTimeArg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATEV2: {
                    UdfConvert.convertArrayDateV2Arg(argument, row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIMEV2: {
                    UdfConvert.convertArrayDateTimeV2Arg(argument, row, currentRowNum, offsetStart, isNullable,
                            nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMALV2:
                case DECIMAL128: {
                    UdfConvert.convertArrayDecimalArg(argTypes[argIdx].getScale(), 16L, argument, row, currentRowNum,
                            offsetStart, isNullable,
                            nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL32: {
                    UdfConvert.convertArrayDecimalArg(argTypes[argIdx].getScale(), 4L, argument, row, currentRowNum,
                            offsetStart, isNullable,
                            nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL64: {
                    UdfConvert.convertArrayDecimalArg(argTypes[argIdx].getScale(), 8L, argument, row, currentRowNum,
                            offsetStart, isNullable,
                            nullMapAddr,
                            nestedNullMapAddr, dataAddr);
                    break;
                }
                default: {
                    LOG.info("Not support: " + argTypes[argIdx]);
                    Preconditions.checkState(false, "Not support type " + argTypes[argIdx].toString());
                    break;
                }
            }
        }
        return argument;
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
                result[i] = method.invoke(udf, parameters);
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
}
