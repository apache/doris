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
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.udf.UdfUtils.JavaUdfDataType;

import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public abstract class BaseExecutor {
    private static final Logger LOG = Logger.getLogger(BaseExecutor.class);

    // By convention, the function in the class must be called evaluate()
    public static final String UDF_FUNCTION_NAME = "evaluate";
    public static final String UDAF_CREATE_FUNCTION = "create";
    public static final String UDAF_DESTROY_FUNCTION = "destroy";
    public static final String UDAF_ADD_FUNCTION = "add";
    public static final String UDAF_SERIALIZE_FUNCTION = "serialize";
    public static final String UDAF_DESERIALIZE_FUNCTION = "deserialize";
    public static final String UDAF_MERGE_FUNCTION = "merge";
    public static final String UDAF_RESULT_FUNCTION = "getValue";

    // Object to deserialize ctor params from BE.
    protected static final TBinaryProtocol.Factory PROTOCOL_FACTORY =
            new TBinaryProtocol.Factory();

    protected Object udf;
    // setup by init() and cleared by close()
    protected URLClassLoader classLoader;

    // Return and argument types of the function inferred from the udf method signature.
    // The JavaUdfDataType enum maps it to corresponding primitive type.
    protected JavaUdfDataType[] argTypes;
    protected JavaUdfDataType retType;

    // Input buffer from the backend. This is valid for the duration of an evaluate() call.
    // These buffers are allocated in the BE.
    protected final long inputBufferPtrs;
    protected final long inputNullsPtrs;
    protected final long inputOffsetsPtrs;

    // Output buffer to return non-string values. These buffers are allocated in the BE.
    protected final long outputBufferPtr;
    protected final long outputNullPtr;
    protected final long outputOffsetsPtr;
    protected final long outputIntermediateStatePtr;
    protected Class[] argClass;

    /**
     * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
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

        outputBufferPtr = request.output_buffer_ptr;
        outputNullPtr = request.output_null_ptr;
        outputOffsetsPtr = request.output_offsets_ptr;
        outputIntermediateStatePtr = request.output_intermediate_state_ptr;

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
                    long numBytes = row == 0 ? offset : offset - Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null,
                                    UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i)) + 4L * (row - 1)));
                    long base =
                            row == 0 ? UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) :
                                    UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                                            + offset - numBytes;
                    byte[] bytes = new byte[(int) numBytes];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, numBytes);
                    inputObjects[i] = new String(bytes, StandardCharsets.UTF_8);
                    break;
                }
                default:
                    throw new UdfRuntimeException("Unsupported argument type: " + argTypes[i]);
            }
        }
        return inputObjects;
    }

    protected abstract long getCurrentOutputOffset(long row);

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
                long offset = getCurrentOutputOffset(row);
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
            default:
                throw new UdfRuntimeException("Unsupported return type: " + retType);
        }
    }

    protected void updateOutputOffset(long offset) {
    }
}
