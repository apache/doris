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
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.JNINativeMethod;
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.esotericsoftware.reflectasm.MethodAccess;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
        String res = "";
        for (JavaUdfDataType type : argTypes) {
            res = res + type.toString();
            if (type.getItemType() != null) {
                res = res + " item: " + type.getItemType().toString() + " sql: " + type.getItemType().toSql();
            }
            if (type.getKeyType() != null) {
                res = res + " key: " + type.getKeyType().toString() + " sql: " + type.getKeyType().toSql();
            }
            if (type.getValueType() != null) {
                res = res + " key: " + type.getValueType().toString() + " sql: " + type.getValueType().toSql();
            }
        }
        res = res + " return type: " + retType.toString();
        if (retType.getItemType() != null) {
            res = res + " item: " + retType.getItemType().toString() + " sql: " + retType.getItemType().toSql();
        }
        if (retType.getKeyType() != null) {
            res = res + " key: " + retType.getKeyType().toString() + " sql: " + retType.getKeyType().toSql();
        }
        if (retType.getValueType() != null) {
            res = res + " key: " + retType.getValueType().toString() + " sql: " + retType.getValueType().toSql();
        }
        res = res + " methodAccess: " + methodAccess.toString();
        res = res + " fn.toString(): " + fn.toString();
        return res;
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

    public void copyTupleBasicResult(Object obj, long row, Class retClass,
            long outputBufferBase, long charsAddress, long offsetsAddr, JavaUdfDataType retType)
            throws UdfRuntimeException {
        switch (retType.getPrimitiveType()) {
            case BOOLEAN: {
                boolean val = (boolean) obj;
                UdfUtils.UNSAFE.putByte(outputBufferBase + row * retType.getLen(),
                        val ? (byte) 1 : 0);
                break;
            }
            case TINYINT: {
                UdfUtils.UNSAFE.putByte(outputBufferBase + row * retType.getLen(),
                        (byte) obj);
                break;
            }
            case SMALLINT: {
                UdfUtils.UNSAFE.putShort(outputBufferBase + row * retType.getLen(),
                        (short) obj);
                break;
            }
            case INT: {
                UdfUtils.UNSAFE.putInt(outputBufferBase + row * retType.getLen(),
                        (int) obj);
                break;
            }
            case BIGINT: {
                UdfUtils.UNSAFE.putLong(outputBufferBase + row * retType.getLen(),
                        (long) obj);
                break;
            }
            case FLOAT: {
                UdfUtils.UNSAFE.putFloat(outputBufferBase + row * retType.getLen(),
                        (float) obj);
                break;
            }
            case DOUBLE: {
                UdfUtils.UNSAFE.putDouble(outputBufferBase + row * retType.getLen(),
                        (double) obj);
                break;
            }
            case DATE: {
                long time = UdfUtils.convertToDate(obj, retClass);
                UdfUtils.UNSAFE.putLong(outputBufferBase + row * retType.getLen(), time);
                break;
            }
            case DATETIME: {
                long time = UdfUtils.convertToDateTime(obj, retClass);
                UdfUtils.UNSAFE.putLong(outputBufferBase + row * retType.getLen(), time);
                break;
            }
            case DATEV2: {
                int time = UdfUtils.convertToDateV2(obj, retClass);
                UdfUtils.UNSAFE.putInt(outputBufferBase + row * retType.getLen(), time);
                break;
            }
            case DATETIMEV2: {
                long time = UdfUtils.convertToDateTimeV2(obj, retClass);
                UdfUtils.UNSAFE.putLong(outputBufferBase + row * retType.getLen(), time);
                break;
            }
            case LARGEINT: {
                BigInteger data = (BigInteger) obj;
                byte[] bytes = UdfUtils.convertByteOrder(data.toByteArray());

                // here value is 16 bytes, so if result data greater than the maximum of 16
                // bytesit will return a wrong num to backend;
                byte[] value = new byte[16];
                // check data is negative
                if (data.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }

                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        outputBufferBase + row * retType.getLen(), value.length);
                break;
            }
            case DECIMALV2: {
                BigDecimal retValue = ((BigDecimal) obj).setScale(9, RoundingMode.HALF_EVEN);
                BigInteger data = retValue.unscaledValue();
                byte[] bytes = UdfUtils.convertByteOrder(data.toByteArray());
                // TODO: here is maybe overflow also, and may find a better way to handle
                byte[] value = new byte[16];
                if (data.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }

                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }

                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        outputBufferBase + row * retType.getLen(), value.length);
                break;
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128I: {
                BigDecimal retValue = ((BigDecimal) obj).setScale(retType.getScale(), RoundingMode.HALF_EVEN);
                BigInteger data = retValue.unscaledValue();
                byte[] bytes = UdfUtils.convertByteOrder(data.toByteArray());
                // TODO: here is maybe overflow also, and may find a better way to handle
                byte[] value = new byte[retType.getLen()];
                if (data.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }

                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }

                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        outputBufferBase + row * retType.getLen(), value.length);
                break;
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                byte[] bytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
                long offset = UdfUtils.UNSAFE.getInt(null, offsetsAddr + 4L * (row - 1));
                int needLen = (int) (offset + bytes.length);
                outputBufferBase = JNINativeMethod.resizeStringColumn(charsAddress, needLen);
                offset += bytes.length;
                UdfUtils.UNSAFE.putInt(null, offsetsAddr + 4L * row, Integer.parseUnsignedInt(String.valueOf(offset)));
                UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null, outputBufferBase + offset - bytes.length,
                        bytes.length);
                break;
            }
            case ARRAY:
            default:
                throw new UdfRuntimeException("Unsupported return type: " + retType);
        }
    }

    public Object[] convertBasicArg(boolean isUdf, int argIdx, boolean isNullable, int rowStart, int rowEnd,
            long nullMapAddr, long columnAddr, long strOffsetAddr) {
        switch (argTypes[argIdx].getPrimitiveType()) {
            case BOOLEAN:
                return UdfConvert.convertBooleanArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case TINYINT:
                return UdfConvert.convertTinyIntArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case SMALLINT:
                return UdfConvert.convertSmallIntArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case INT:
                return UdfConvert.convertIntArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case BIGINT:
                return UdfConvert.convertBigIntArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case LARGEINT:
                return UdfConvert.convertLargeIntArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case FLOAT:
                return UdfConvert.convertFloatArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case DOUBLE:
                return UdfConvert.convertDoubleArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr);
            case CHAR:
            case VARCHAR:
            case STRING:
                return UdfConvert
                        .convertStringArg(isNullable, rowStart, rowEnd, nullMapAddr, columnAddr, strOffsetAddr);
            case DATE: // udaf maybe argClass[i + argClassOffset] need add +1
                return UdfConvert
                        .convertDateArg(isUdf ? argClass[argIdx] : argClass[argIdx + 1], isNullable, rowStart, rowEnd,
                                nullMapAddr, columnAddr);
            case DATETIME:
                return UdfConvert
                        .convertDateTimeArg(isUdf ? argClass[argIdx] : argClass[argIdx + 1], isNullable, rowStart,
                                rowEnd, nullMapAddr, columnAddr);
            case DATEV2:
                return UdfConvert
                        .convertDateV2Arg(isUdf ? argClass[argIdx] : argClass[argIdx + 1], isNullable, rowStart, rowEnd,
                                nullMapAddr, columnAddr);
            case DATETIMEV2:
                return UdfConvert
                        .convertDateTimeV2Arg(isUdf ? argClass[argIdx] : argClass[argIdx + 1], isNullable, rowStart,
                                rowEnd, nullMapAddr, columnAddr);
            case DECIMALV2:
            case DECIMAL128I:
                return UdfConvert
                        .convertDecimalArg(argTypes[argIdx].getScale(), 16L, isNullable, rowStart, rowEnd, nullMapAddr,
                                columnAddr);
            case DECIMAL32:
                return UdfConvert
                        .convertDecimalArg(argTypes[argIdx].getScale(), 4L, isNullable, rowStart, rowEnd, nullMapAddr,
                                columnAddr);
            case DECIMAL64:
                return UdfConvert
                        .convertDecimalArg(argTypes[argIdx].getScale(), 8L, isNullable, rowStart, rowEnd, nullMapAddr,
                                columnAddr);
            default: {
                LOG.info("Not support type: " + argTypes[argIdx].toString());
                Preconditions.checkState(false, "Not support type: " + argTypes[argIdx].toString());
                break;
            }
        }
        return null;
    }

    public Object[] convertArrayArg(int argIdx, boolean isNullable, int rowStart, int rowEnd, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        Object[] argument = (Object[]) Array.newInstance(ArrayList.class, rowEnd - rowStart);
        for (int row = rowStart; row < rowEnd; ++row) {
            long offsetStart = UdfUtils.UNSAFE.getLong(null, offsetsAddr + 8L * (row - 1));
            long offsetEnd = UdfUtils.UNSAFE.getLong(null, offsetsAddr + 8L * (row));
            int currentRowNum = (int) (offsetEnd - offsetStart);
            switch (argTypes[argIdx].getItemType().getPrimitiveType()) {
                case BOOLEAN: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayBooleanArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case TINYINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayTinyIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case SMALLINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArraySmallIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case INT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case BIGINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayBigIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case LARGEINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayLargeIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case FLOAT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayFloatArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DOUBLE: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDoubleArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayStringArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr, strOffsetAddr);
                    break;
                }
                case DATE: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIME: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateTimeArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATEV2: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateV2Arg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIMEV2: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateTimeV2Arg(row, currentRowNum, offsetStart, isNullable,
                                    nullMapAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMALV2:
                case DECIMAL128: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDecimalArg(argTypes[argIdx].getScale(), 16L, row, currentRowNum,
                                    offsetStart, isNullable, nullMapAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL32: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDecimalArg(argTypes[argIdx].getScale(), 4L, row, currentRowNum,
                                    offsetStart, isNullable, nullMapAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL64: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDecimalArg(argTypes[argIdx].getScale(), 8L, row, currentRowNum,
                                    offsetStart, isNullable, nullMapAddr, nestedNullMapAddr, dataAddr);
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

    public Object[] convertMapArg(PrimitiveType type, int argIdx, boolean isNullable, int rowStart, int rowEnd,
            long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr, int scale) {
        Object[] argument = (Object[]) Array.newInstance(ArrayList.class, rowEnd - rowStart);
        for (int row = rowStart; row < rowEnd; ++row) {
            long offsetStart = UdfUtils.UNSAFE.getLong(null, offsetsAddr + 8L * (row - 1));
            long offsetEnd = UdfUtils.UNSAFE.getLong(null, offsetsAddr + 8L * (row));
            int currentRowNum = (int) (offsetEnd - offsetStart);
            switch (type) {
                case BOOLEAN: {
                    argument[row
                            - rowStart] = UdfConvert
                                    .convertArrayBooleanArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                            nestedNullMapAddr, dataAddr);
                    break;
                }
                case TINYINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayTinyIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case SMALLINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArraySmallIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case INT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case BIGINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayBigIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case LARGEINT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayLargeIntArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case FLOAT: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayFloatArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DOUBLE: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDoubleArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayStringArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr, strOffsetAddr);
                    break;
                }
                case DATE: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIME: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateTimeArg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATEV2: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateV2Arg(row, currentRowNum, offsetStart, isNullable, nullMapAddr,
                                    nestedNullMapAddr, dataAddr);
                    break;
                }
                case DATETIMEV2: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDateTimeV2Arg(row, currentRowNum, offsetStart, isNullable,
                                    nullMapAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMALV2:
                case DECIMAL128: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDecimalArg(scale, 16L, row, currentRowNum,
                                    offsetStart, isNullable, nullMapAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL32: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDecimalArg(scale, 4L, row, currentRowNum,
                                    offsetStart, isNullable, nullMapAddr, nestedNullMapAddr, dataAddr);
                    break;
                }
                case DECIMAL64: {
                    argument[row - rowStart] = UdfConvert
                            .convertArrayDecimalArg(scale, 8L, row, currentRowNum,
                                    offsetStart, isNullable, nullMapAddr, nestedNullMapAddr, dataAddr);
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

    public Object[] buildHashMap(PrimitiveType keyType, PrimitiveType valueType, Object[] keyCol, Object[] valueCol) {
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

    public void copyBatchBasicResultImpl(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long resColumnAddr, long strOffsetAddr, Method method) {
        switch (retType.getPrimitiveType()) {
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
            case DECIMAL128I: {
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

    public void copyBatchArrayResultImpl(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr,
            PrimitiveType type, int scale) {
        long hasPutElementNum = 0;
        for (int row = 0; row < numRows; ++row) {
            hasPutElementNum = copyTupleArrayResultImpl(hasPutElementNum, isNullable, row, result[row], nullMapAddr,
                    offsetsAddr, nestedNullMapAddr, dataAddr, strOffsetAddr, type, scale);
        }
    }

    public long copyTupleArrayResultImpl(long hasPutElementNum, boolean isNullable, int row, Object result,
            long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr,
            PrimitiveType type, int scale) {
        switch (type) {
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
                        .copyBatchArrayDecimalV3Result(scale, 4L, hasPutElementNum, isNullable, row,
                                result, nullMapAddr,
                                offsetsAddr, nestedNullMapAddr, dataAddr);
                break;
            }
            case DECIMAL64: {
                hasPutElementNum = UdfConvert
                        .copyBatchArrayDecimalV3Result(scale, 8L, hasPutElementNum, isNullable, row,
                                result, nullMapAddr,
                                offsetsAddr, nestedNullMapAddr, dataAddr);
                break;
            }
            case DECIMAL128: {
                hasPutElementNum = UdfConvert
                        .copyBatchArrayDecimalV3Result(scale, 16L, hasPutElementNum, isNullable, row,
                                result, nullMapAddr,
                                offsetsAddr, nestedNullMapAddr, dataAddr);
                break;
            }
            default: {
                Preconditions.checkState(false, "Not support type in array: " + retType);
                break;
            }
        }
        return hasPutElementNum;
    }

    public void buildArrayListFromHashMap(Object[] result, PrimitiveType keyType, PrimitiveType valueType,
            Object[] keyCol, Object[] valueCol) {
        switch (keyType) {
            case BOOLEAN: {
                new ArrayListBuilder<Boolean>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case TINYINT: {
                new ArrayListBuilder<Byte>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case SMALLINT: {
                new ArrayListBuilder<Short>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case INT: {
                new ArrayListBuilder<Integer>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case BIGINT: {
                new ArrayListBuilder<Long>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case LARGEINT: {
                new ArrayListBuilder<BigInteger>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case FLOAT: {
                new ArrayListBuilder<Float>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case DOUBLE: {
                new ArrayListBuilder<Double>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                new ArrayListBuilder<String>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case DATEV2:
            case DATE: {
                new ArrayListBuilder<LocalDate>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case DATETIMEV2:
            case DATETIME: {
                new ArrayListBuilder<LocalDateTime>().get(result, keyCol, valueCol, valueType);
                break;
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMALV2:
            case DECIMAL128: {
                new ArrayListBuilder<BigDecimal>().get(result, keyCol, valueCol, valueType);
                break;
            }
            default: {
                LOG.info("Not support: " + keyType);
                Preconditions.checkState(false, "Not support type " + keyType.toString());
                break;
            }
        }
    }

    public static class ArrayListBuilder<keyType> {
        public void get(Object[] map, Object[] keyCol, Object[] valueCol, PrimitiveType valueType) {
            switch (valueType) {
                case BOOLEAN: {
                    new BuildArrayFromType<keyType, Boolean>().get(map, keyCol, valueCol);
                    break;
                }
                case TINYINT: {
                    new BuildArrayFromType<keyType, Byte>().get(map, keyCol, valueCol);
                    break;
                }
                case SMALLINT: {
                    new BuildArrayFromType<keyType, Short>().get(map, keyCol, valueCol);
                    break;
                }
                case INT: {
                    new BuildArrayFromType<keyType, Integer>().get(map, keyCol, valueCol);
                    break;
                }
                case BIGINT: {
                    new BuildArrayFromType<keyType, Long>().get(map, keyCol, valueCol);
                    break;
                }
                case LARGEINT: {
                    new BuildArrayFromType<keyType, BigInteger>().get(map, keyCol, valueCol);
                    break;
                }
                case FLOAT: {
                    new BuildArrayFromType<keyType, Float>().get(map, keyCol, valueCol);
                    break;
                }
                case DOUBLE: {
                    new BuildArrayFromType<keyType, Double>().get(map, keyCol, valueCol);
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    new BuildArrayFromType<keyType, String>().get(map, keyCol, valueCol);
                    break;
                }
                case DATEV2:
                case DATE: {
                    new BuildArrayFromType<keyType, LocalDate>().get(map, keyCol, valueCol);
                    break;
                }
                case DATETIMEV2:
                case DATETIME: {
                    new BuildArrayFromType<keyType, LocalDateTime>().get(map, keyCol, valueCol);
                    break;
                }
                case DECIMAL32:
                case DECIMAL64:
                case DECIMALV2:
                case DECIMAL128: {
                    new BuildArrayFromType<keyType, BigDecimal>().get(map, keyCol, valueCol);
                    break;
                }
                default: {
                    LOG.info("Not support: " + valueType);
                    Preconditions.checkState(false, "Not support type " + valueType.toString());
                    break;
                }
            }
        }
    }

    public static class BuildArrayFromType<T1, T2> {
        public void get(Object[] map, Object[] keyCol, Object[] valueCol) {
            for (int colIdx = 0; colIdx < map.length; colIdx++) {
                HashMap<T1, T2> hashMap = (HashMap<T1, T2>) map[colIdx];
                ArrayList<T1> keys = new ArrayList<>();
                ArrayList<T2> values = new ArrayList<>();
                for (Map.Entry<T1, T2> entry : hashMap.entrySet()) {
                    keys.add(entry.getKey());
                    values.add(entry.getValue());
                }
                keyCol[colIdx] = keys;
                valueCol[colIdx] = values;
            }
        }
    }
}
