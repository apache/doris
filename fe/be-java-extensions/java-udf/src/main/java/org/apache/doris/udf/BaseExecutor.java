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
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.utils.UdfUtils.JavaUdfDataType;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;

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

    // Input buffer from the backend. This is valid for the duration of an
    // evaluate() call.
    // These buffers are allocated in the BE.
    protected final long inputBufferPtrs;
    protected final long inputNullsPtrs;
    protected final long inputOffsetsPtrs;
    protected final long inputArrayNullsPtrs;
    protected final long inputArrayStringOffsetsPtrs;

    // Output buffer to return non-string values. These buffers are allocated in the
    // BE.
    protected final long outputBufferPtr;
    protected final long outputNullPtr;
    protected final long outputOffsetsPtr;
    protected final long outputArrayNullPtr;
    protected final long outputArrayStringOffsetsPtr;
    protected final long outputIntermediateStatePtr;
    protected Class[] argClass;

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
        inputBufferPtrs = request.input_buffer_ptrs;
        inputNullsPtrs = request.input_nulls_ptrs;
        inputOffsetsPtrs = request.input_offsets_ptrs;
        inputArrayNullsPtrs = request.input_array_nulls_buffer_ptr;
        inputArrayStringOffsetsPtrs = request.input_array_string_offsets_ptrs;
        outputBufferPtr = request.output_buffer_ptr;
        outputNullPtr = request.output_null_ptr;
        outputOffsetsPtr = request.output_offsets_ptr;
        outputIntermediateStatePtr = request.output_intermediate_state_ptr;
        outputArrayNullPtr = request.output_array_null_ptr;
        outputArrayStringOffsetsPtr = request.output_array_string_offsets_ptr;

        Type[] parameterTypes = new Type[request.fn.arg_types.size()];
        for (int i = 0; i < request.fn.arg_types.size(); ++i) {
            parameterTypes[i] = Type.fromThrift(request.fn.arg_types.get(i));
        }
        String jarFile = request.location;
        Type funcRetType = UdfUtils.fromThrift(request.fn.ret_type, 0).first;

        init(request, jarFile, funcRetType, parameterTypes);
    }

    protected abstract void init(TJavaUdfExecutorCtorParams request, String jarPath,
            Type funcRetType, Type... parameterTypes) throws UdfRuntimeException;

    protected Object[] allocateInputObjects(long row, int argClassOffset) throws UdfRuntimeException {
        Object[] inputObjects = new Object[argTypes.length];

        for (int i = 0; i < argTypes.length; ++i) {
            if (UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputNullsPtrs, i)) != -1
                    && (UdfUtils.UNSAFE.getByte(null, UdfUtils.UNSAFE.getLong(null,
                    UdfUtils.getAddressAtOffset(inputNullsPtrs, i)) + row) == 1)) {
                inputObjects[i] = null;
                continue;
            }
            switch (argTypes[i]) {
                case BOOLEAN:
                    inputObjects[i] = UdfUtils.UNSAFE.getBoolean(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + row);
                    break;
                case TINYINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getByte(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + row);
                    break;
                case SMALLINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getShort(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    break;
                case INT:
                    inputObjects[i] = UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    break;
                case BIGINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    break;
                case FLOAT:
                    inputObjects[i] = UdfUtils.UNSAFE.getFloat(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    break;
                case DOUBLE:
                    inputObjects[i] = UdfUtils.UNSAFE.getDouble(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    break;
                case DATE: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    inputObjects[i] = UdfUtils.convertDateToJavaDate(data, argClass[i + argClassOffset]);
                    break;
                }
                case DATETIME: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    inputObjects[i] = UdfUtils.convertDateTimeToJavaDateTime(data, argClass[i + argClassOffset]);
                    break;
                }
                case DATEV2: {
                    int data = UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    inputObjects[i] = UdfUtils.convertDateV2ToJavaDate(data, argClass[i + argClassOffset]);
                    break;
                }
                case DATETIMEV2: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + argTypes[i].getLen() * row);
                    inputObjects[i] = UdfUtils.convertDateTimeV2ToJavaDateTime(data, argClass[i + argClassOffset]);
                    break;
                }
                case LARGEINT: {
                    long base = UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                            + argTypes[i].getLen() * row;
                    byte[] bytes = new byte[argTypes[i].getLen()];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, argTypes[i].getLen());

                    inputObjects[i] = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    break;
                }
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128: {
                    long base = UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                            + argTypes[i].getLen() * row;
                    byte[] bytes = new byte[argTypes[i].getLen()];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, argTypes[i].getLen());

                    BigInteger value = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    inputObjects[i] = new BigDecimal(value, argTypes[i].getScale());
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    long offset = Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i)) + 4L * row));
                    long numBytes = row == 0 ? offset
                            : offset - Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                                    UdfUtils.UNSAFE.getLong(null,
                                            UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i)) + 4L * (row - 1)));
                    long base = row == 0
                            ? UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                            : UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                    + offset - numBytes;
                    byte[] bytes = new byte[(int) numBytes];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, numBytes);
                    inputObjects[i] = new String(bytes, StandardCharsets.UTF_8);
                    break;
                }
                case ARRAY_TYPE: {
                    Type type = argTypes[i].getItemType();
                    inputObjects[i] = arrayTypeInputData(type, i, row);
                    break;
                }
                default:
                    throw new UdfRuntimeException("Unsupported argument type: " + argTypes[i]);
            }
        }
        return inputObjects;
    }

    public ArrayList<?> arrayTypeInputData(Type type, int argIdx, long row)
            throws UdfRuntimeException {
        long offsetStart = (row == 0) ? 0
                : Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null,
                        UdfUtils.getAddressAtOffset(inputOffsetsPtrs, argIdx)) + 8L * (row - 1)));
        long offsetEnd = Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null,
                UdfUtils.getAddressAtOffset(inputOffsetsPtrs, argIdx)) + 8L * row));
        long arrayNullMapBase = UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputArrayNullsPtrs, argIdx));
        long arrayInputBufferBase = UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, argIdx));

        switch (type.getPrimitiveType()) {
            case BOOLEAN: {
                ArrayList<Boolean> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        boolean value = UdfUtils.UNSAFE.getBoolean(null, arrayInputBufferBase + offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case TINYINT: {
                ArrayList<Byte> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        byte value = UdfUtils.UNSAFE.getByte(null, arrayInputBufferBase + offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case SMALLINT: {
                ArrayList<Short> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        short value = UdfUtils.UNSAFE.getShort(null, arrayInputBufferBase + 2L * offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case INT: {
                ArrayList<Integer> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        int value = UdfUtils.UNSAFE.getInt(null, arrayInputBufferBase + 4L * offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case BIGINT: {
                ArrayList<Long> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long value = UdfUtils.UNSAFE.getLong(null, arrayInputBufferBase + 8L * offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case FLOAT: {
                ArrayList<Float> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        float value = UdfUtils.UNSAFE.getFloat(null, arrayInputBufferBase + 4L * offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case DOUBLE: {
                ArrayList<Double> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        double value = UdfUtils.UNSAFE.getDouble(null, arrayInputBufferBase + 8L * offsetRow);
                        data.add(value);
                    }
                }
                return data;
            }
            case DATE: {
                ArrayList<LocalDate> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long value = UdfUtils.UNSAFE.getLong(null, arrayInputBufferBase + 8L * offsetRow);
                        // TODO: now argClass[argIdx + argClassOffset] is java.util.ArrayList, can't get
                        // nested class type
                        // LocalDate obj = UdfUtils.convertDateToJavaDate(value, argClass[argIdx +
                        // argClassOffset]);
                        LocalDate obj = (LocalDate) UdfUtils.convertDateToJavaDate(value, LocalDate.class);
                        data.add(obj);
                    }
                }
                return data;
            }
            case DATETIME: {
                ArrayList<LocalDateTime> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long value = UdfUtils.UNSAFE.getLong(null, arrayInputBufferBase + 8L * offsetRow);
                        // Object obj = UdfUtils.convertDateTimeToJavaDateTime(value, argClass[argIdx +
                        // argClassOffset]);
                        LocalDateTime obj = (LocalDateTime) UdfUtils.convertDateTimeToJavaDateTime(value,
                                LocalDateTime.class);
                        data.add(obj);
                    }
                }
                return data;
            }
            case DATEV2: {
                ArrayList<LocalDate> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        int value = UdfUtils.UNSAFE.getInt(null, arrayInputBufferBase + 4L * offsetRow);
                        // Object obj = UdfUtils.convertDateV2ToJavaDate(value, argClass[argIdx +
                        // argClassOffset]);
                        LocalDate obj = (LocalDate) UdfUtils.convertDateV2ToJavaDate(value, LocalDate.class);
                        data.add(obj);
                    }
                }
                return data;
            }
            case DATETIMEV2: {
                ArrayList<LocalDateTime> data = new ArrayList<>();
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long value = UdfUtils.UNSAFE.getLong(null, arrayInputBufferBase + 8L * offsetRow);
                        LocalDateTime obj = (LocalDateTime) UdfUtils.convertDateTimeV2ToJavaDateTime(value,
                                LocalDateTime.class);
                        data.add(obj);
                    }
                }
                return data;
            }
            case LARGEINT: {
                ArrayList<BigInteger> data = new ArrayList<>();
                byte[] bytes = new byte[16];
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long value = UdfUtils.UNSAFE.getLong(null, arrayInputBufferBase + 16L * offsetRow);
                        UdfUtils.copyMemory(null, value, bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);
                        data.add(new BigInteger(UdfUtils.convertByteOrder(bytes)));
                    }
                }
                return data;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                int len;
                if (type.getPrimitiveType() == PrimitiveType.DECIMAL32) {
                    len = 4;
                } else if (type.getPrimitiveType() == PrimitiveType.DECIMAL64) {
                    len = 8;
                } else {
                    len = 16;
                }
                ArrayList<BigDecimal> data = new ArrayList<>();
                byte[] bytes = new byte[len];
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long value = UdfUtils.UNSAFE.getLong(null, arrayInputBufferBase + len * offsetRow);
                        UdfUtils.copyMemory(null, value, bytes, UdfUtils.BYTE_ARRAY_OFFSET, len);
                        BigInteger bigInteger = new BigInteger(UdfUtils.convertByteOrder(bytes));
                        data.add(new BigDecimal(bigInteger, argTypes[argIdx].getScale()));
                    }
                }
                return data;
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                ArrayList<String> data = new ArrayList<>();
                long strOffsetBase = UdfUtils.UNSAFE
                        .getLong(null, UdfUtils.getAddressAtOffset(inputArrayStringOffsetsPtrs, argIdx));
                for (long offsetRow = offsetStart; offsetRow < offsetEnd; ++offsetRow) {
                    if ((UdfUtils.UNSAFE.getByte(null, arrayNullMapBase + offsetRow) == 1)) {
                        data.add(null);
                    } else {
                        long stringOffsetStart = (offsetRow == 0) ? 0
                                : Integer.toUnsignedLong(
                                        UdfUtils.UNSAFE.getInt(null, strOffsetBase + 4L * (offsetRow - 1)));
                        long stringOffsetEnd = Integer
                                .toUnsignedLong(UdfUtils.UNSAFE.getInt(null, strOffsetBase + 4L * offsetRow));

                        long numBytes = stringOffsetEnd - stringOffsetStart;
                        long base = arrayInputBufferBase + stringOffsetStart;
                        byte[] bytes = new byte[(int) numBytes];
                        UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, numBytes);
                        data.add(new String(bytes, StandardCharsets.UTF_8));
                    }
                }
                return data;
            }
            default:
                throw new UdfRuntimeException("Unsupported argument type in nested array: " + type);
        }
    }

    protected abstract long getCurrentOutputOffset(long row, boolean isArrayType);

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

    // Sets the result object 'obj' into the outputBufferPtr and outputNullPtr_
    protected boolean storeUdfResult(Object obj, long row, Class retClass) throws UdfRuntimeException {
        if (UdfUtils.UNSAFE.getLong(null, outputNullPtr) != -1) {
            UdfUtils.UNSAFE.putByte(UdfUtils.UNSAFE.getLong(null, outputNullPtr) + row, (byte) 0);
        }
        switch (retType) {
            case BOOLEAN: {
                boolean val = (boolean) obj;
                UdfUtils.UNSAFE.putByte(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        val ? (byte) 1 : 0);
                return true;
            }
            case TINYINT: {
                UdfUtils.UNSAFE.putByte(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        (byte) obj);
                return true;
            }
            case SMALLINT: {
                UdfUtils.UNSAFE.putShort(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        (short) obj);
                return true;
            }
            case INT: {
                UdfUtils.UNSAFE.putInt(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        (int) obj);
                return true;
            }
            case BIGINT: {
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        (long) obj);
                return true;
            }
            case FLOAT: {
                UdfUtils.UNSAFE.putFloat(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        (float) obj);
                return true;
            }
            case DOUBLE: {
                UdfUtils.UNSAFE.putDouble(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(),
                        (double) obj);
                return true;
            }
            case DATE: {
                long time = UdfUtils.convertToDate(obj, retClass);
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case DATETIME: {
                long time = UdfUtils.convertToDateTime(obj, retClass);
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case DATEV2: {
                int time = UdfUtils.convertToDateV2(obj, retClass);
                UdfUtils.UNSAFE.putInt(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case DATETIMEV2: {
                long time = UdfUtils.convertToDateTimeV2(obj, retClass);
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case LARGEINT: {
                BigInteger data = (BigInteger) obj;
                byte[] bytes = UdfUtils.convertByteOrder(data.toByteArray());

                //here value is 16 bytes, so if result data greater than the maximum of 16 bytes
                //it will return a wrong num to backend;
                byte[] value = new byte[16];
                //check data is negative
                if (data.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }
                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }

                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), value.length);
                return true;
            }
            case DECIMALV2: {
                BigDecimal retValue = ((BigDecimal) obj).setScale(9, RoundingMode.HALF_EVEN);
                BigInteger data = retValue.unscaledValue();
                byte[] bytes = UdfUtils.convertByteOrder(data.toByteArray());
                //TODO: here is maybe overflow also, and may find a better way to handle
                byte[] value = new byte[16];
                if (data.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }

                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }

                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), value.length);
                return true;
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                BigDecimal retValue = ((BigDecimal) obj).setScale(retType.getScale(), RoundingMode.HALF_EVEN);
                BigInteger data = retValue.unscaledValue();
                byte[] bytes = UdfUtils.convertByteOrder(data.toByteArray());
                //TODO: here is maybe overflow also, and may find a better way to handle
                byte[] value = new byte[retType.getLen()];
                if (data.signum() == -1) {
                    Arrays.fill(value, (byte) -1);
                }

                for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                    value[index] = bytes[index];
                }

                UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), value.length);
                return true;
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                long bufferSize = UdfUtils.UNSAFE.getLong(null, outputIntermediateStatePtr);
                byte[] bytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
                long offset = getCurrentOutputOffset(row, false);
                if (offset + bytes.length > bufferSize) {
                    return false;
                }
                offset += bytes.length;
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 4L * row,
                        Integer.parseUnsignedInt(String.valueOf(offset)));
                UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + offset - bytes.length, bytes.length);
                updateOutputOffset(offset);
                return true;
            }
            case ARRAY_TYPE: {
                Type type = retType.getItemType();
                return arrayTypeOutputData(obj, type, row);
            }
            default:
                throw new UdfRuntimeException("Unsupported return type: " + retType);
        }
    }

    public boolean arrayTypeOutputData(Object obj, Type type, long row) throws UdfRuntimeException {
        long offset = getCurrentOutputOffset(row, true);
        long bufferSize = UdfUtils.UNSAFE.getLong(null, outputIntermediateStatePtr);
        long outputNullMapBase = UdfUtils.UNSAFE.getLong(null, outputArrayNullPtr);
        long outputBufferBase = UdfUtils.UNSAFE.getLong(null, outputBufferPtr);
        switch (type.getPrimitiveType()) {
            case BOOLEAN: {
                ArrayList<Boolean> data = (ArrayList<Boolean>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Boolean value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putByte(outputBufferBase + (offset + i), value ? (byte) 1 : 0);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case TINYINT: {
                ArrayList<Byte> data = (ArrayList<Byte>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Byte value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putByte(outputBufferBase + (offset + i), value);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case SMALLINT: {
                ArrayList<Short> data = (ArrayList<Short>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Short value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putShort(outputBufferBase + ((offset + i) * 2L), value);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case INT: {
                ArrayList<Integer> data = (ArrayList<Integer>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Integer value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putInt(outputBufferBase + ((offset + i) * 4L), value);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case BIGINT: {
                ArrayList<Long> data = (ArrayList<Long>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Long value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putLong(outputBufferBase + ((offset + i) * 8L), value);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case FLOAT: {
                ArrayList<Float> data = (ArrayList<Float>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Float value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putFloat(outputBufferBase + ((offset + i) * 4L), value);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DOUBLE: {
                ArrayList<Double> data = (ArrayList<Double>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    Double value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        UdfUtils.UNSAFE.putDouble(outputBufferBase + ((offset + i) * 8L), value);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DATE: {
                ArrayList<LocalDate> data = (ArrayList<LocalDate>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    LocalDate value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        long time = UdfUtils.convertToDate(value, LocalDate.class);
                        UdfUtils.UNSAFE.putLong(outputBufferBase + ((offset + i) * 8L), time);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DATETIME: {
                ArrayList<LocalDateTime> data = (ArrayList<LocalDateTime>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    LocalDateTime value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        long time = UdfUtils.convertToDateTime(value, LocalDateTime.class);
                        UdfUtils.UNSAFE.putLong(outputBufferBase + ((offset + i) * 8L), time);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DATEV2: {
                ArrayList<LocalDate> data = (ArrayList<LocalDate>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    LocalDate value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        int time = UdfUtils.convertToDateV2(value, LocalDate.class);
                        UdfUtils.UNSAFE.putInt(outputBufferBase + ((offset + i) * 4L), time);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DATETIMEV2: {
                ArrayList<LocalDateTime> data = (ArrayList<LocalDateTime>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    LocalDateTime value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        long time = UdfUtils.convertToDateTimeV2(value, LocalDateTime.class);
                        UdfUtils.UNSAFE.putLong(outputBufferBase + ((offset + i) * 8L), time);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case LARGEINT: {
                ArrayList<BigInteger> data = (ArrayList<BigInteger>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    BigInteger bigInteger = data.get(i);
                    if (bigInteger == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                        byte[] value = new byte[16];
                        // check data is negative
                        if (bigInteger.signum() == -1) {
                            Arrays.fill(value, (byte) -1);
                        }
                        for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                            value[index] = bytes[index];
                        }
                        UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                outputBufferBase + ((offset + i) * 16L), value.length);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DECIMALV2: {
                ArrayList<BigDecimal> data = (ArrayList<BigDecimal>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    BigDecimal bigDecimal = data.get(i);
                    if (bigDecimal == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        BigInteger bigInteger = bigDecimal.setScale(9, RoundingMode.HALF_EVEN).unscaledValue();
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                        byte[] value = new byte[16];
                        // check data is negative
                        if (bigInteger.signum() == -1) {
                            Arrays.fill(value, (byte) -1);
                        }
                        for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                            value[index] = bytes[index];
                        }
                        UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                outputBufferBase + ((offset + i) * 16L), value.length);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128: {
                ArrayList<BigDecimal> data = (ArrayList<BigDecimal>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                for (int i = 0; i < num; ++i) {
                    BigDecimal bigDecimal = data.get(i);
                    if (bigDecimal == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        BigInteger bigInteger = bigDecimal.setScale(retType.getScale(), RoundingMode.HALF_EVEN)
                                .unscaledValue();
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        byte[] bytes = UdfUtils.convertByteOrder(bigInteger.toByteArray());
                        byte[] value = new byte[16];
                        // check data is negative
                        if (bigInteger.signum() == -1) {
                            Arrays.fill(value, (byte) -1);
                        }
                        for (int index = 0; index < Math.min(bytes.length, value.length); ++index) {
                            value[index] = bytes[index];
                        }
                        UdfUtils.copyMemory(value, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                outputBufferBase + ((offset + i) * 16L), value.length);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            case CHAR:
            case VARCHAR:
            case STRING: {
                ArrayList<String> data = (ArrayList<String>) obj;
                int num = data.size();
                if (offset + num > bufferSize) {
                    return false;
                }
                long outputStrOffsetBase = UdfUtils.UNSAFE.getLong(null, outputArrayStringOffsetsPtr);
                for (int i = 0; i < num; ++i) {
                    String value = data.get(i);
                    if (value == null) {
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 1);
                    } else {
                        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                        long strOffset = (offset + i == 0) ? 0
                                : Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                                        outputStrOffsetBase + ((offset + i - 1) * 4L)));
                        if (strOffset + bytes.length > bufferSize) {
                            return false;
                        }
                        UdfUtils.UNSAFE.putByte(outputNullMapBase + (offset + i), (byte) 0);
                        strOffset += bytes.length;
                        UdfUtils.UNSAFE.putInt(null, outputStrOffsetBase + 4L * (offset + i),
                                Integer.parseUnsignedInt(String.valueOf(strOffset)));
                        UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null,
                                outputBufferBase + strOffset - bytes.length, bytes.length);
                    }
                }
                offset += num;
                UdfUtils.UNSAFE.putLong(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 8L * row,
                        Long.parseUnsignedLong(String.valueOf(offset)));
                updateOutputOffset(offset);
                return true;
            }
            default:
                throw new UdfRuntimeException("Unsupported argument type in nested array: " + type);
        }
    }

    protected void updateOutputOffset(long offset) {
    }

    public Object[] convertBasicArg(boolean isUdf, int argIdx, boolean isNullable, int rowStart, int rowEnd,
            long nullMapAddr, long columnAddr, long strOffsetAddr) {
        switch (argTypes[argIdx]) {
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
            case DECIMAL128:
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
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
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
}
