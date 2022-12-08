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
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.udf.UdfUtils.JavaUdfDataType;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * udaf executor.
 */
public class UdafExecutor {
    public static final String UDAF_CREATE_FUNCTION = "create";
    public static final String UDAF_DESTROY_FUNCTION = "destroy";
    public static final String UDAF_ADD_FUNCTION = "add";
    public static final String UDAF_SERIALIZE_FUNCTION = "serialize";
    public static final String UDAF_DESERIALIZE_FUNCTION = "deserialize";
    public static final String UDAF_MERGE_FUNCTION = "merge";
    public static final String UDAF_RESULT_FUNCTION = "getValue";
    private static final Logger LOG = Logger.getLogger(UdafExecutor.class);
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY = new TBinaryProtocol.Factory();
    private final long inputBufferPtrs;
    private final long inputNullsPtrs;
    private final long inputOffsetsPtrs;
    private final long inputPlacesPtr;
    private final long outputBufferPtr;
    private final long outputNullPtr;
    private final long outputOffsetsPtr;
    private final long outputIntermediateStatePtr;
    private Object udaf;
    private HashMap<String, Method> allMethods;
    private HashMap<Long, Object> stateObjMap;
    private URLClassLoader classLoader;
    private JavaUdfDataType[] argTypes;
    private JavaUdfDataType retType;
    private Class[] argClass;
    private Class retClass;

    /**
     * Constructor to create an object.
     */
    public UdafExecutor(byte[] thriftParams) throws Exception {
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
        inputBufferPtrs = request.input_buffer_ptrs;
        inputNullsPtrs = request.input_nulls_ptrs;
        inputOffsetsPtrs = request.input_offsets_ptrs;
        inputPlacesPtr = request.input_places_ptr;

        outputBufferPtr = request.output_buffer_ptr;
        outputNullPtr = request.output_null_ptr;
        outputOffsetsPtr = request.output_offsets_ptr;
        outputIntermediateStatePtr = request.output_intermediate_state_ptr;
        allMethods = new HashMap<>();
        stateObjMap = new HashMap<>();
        String className = request.fn.aggregate_fn.symbol;
        String jarFile = request.location;
        Type funcRetType = UdfUtils.fromThrift(request.fn.ret_type, 0).first;
        init(jarFile, className, funcRetType, parameterTypes);
    }

    /**
     * close and invoke destroy function.
     */
    public void close() {
        if (classLoader != null) {
            try {
                classLoader.close();
            } catch (Exception e) {
                // Log and ignore.
                LOG.debug("Error closing the URLClassloader.", e);
            }
        }
        // We are now un-usable (because the class loader has been
        // closed), so null out allMethods and classLoader.
        allMethods = null;
        classLoader = null;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * invoke add function, add row in loop [rowStart, rowEnd).
     */
    public void add(boolean isSinglePlace, long rowStart, long rowEnd) throws UdfRuntimeException {
        try {
            long idx = rowStart;
            do {
                Long curPlace = UdfUtils.UNSAFE.getLong(null, UdfUtils.UNSAFE.getLong(null, inputPlacesPtr) + 8L * idx);
                Object[] inputArgs = new Object[argTypes.length + 1];
                stateObjMap.putIfAbsent(curPlace, createAggState());
                inputArgs[0] = stateObjMap.get(curPlace);
                do {
                    Object[] inputObjects = allocateInputObjects(idx);
                    for (int i = 0; i < argTypes.length; ++i) {
                        inputArgs[i + 1] = inputObjects[i];
                    }
                    allMethods.get(UDAF_ADD_FUNCTION).invoke(udaf, inputArgs);
                    idx++;
                } while (isSinglePlace && idx < rowEnd);
            } while (idx < rowEnd);
        } catch (Exception e) {
            throw new UdfRuntimeException("UDAF failed to add: ", e);
        }
    }

    /**
     * invoke user create function to get obj.
     */
    public Object createAggState() throws UdfRuntimeException {
        try {
            return allMethods.get(UDAF_CREATE_FUNCTION).invoke(udaf, null);
        } catch (Exception e) {
            throw new UdfRuntimeException("UDAF failed to create: ", e);
        }
    }

    /**
     * invoke destroy before colse. Here we destroy all data at once
     */
    public void destroy() throws UdfRuntimeException {
        try {
            for (Object obj : stateObjMap.values()) {
                allMethods.get(UDAF_DESTROY_FUNCTION).invoke(udaf, obj);
            }
            stateObjMap.clear();
        } catch (Exception e) {
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
            allMethods.get(UDAF_SERIALIZE_FUNCTION).invoke(udaf, args);
            return baos.toByteArray();
        } catch (Exception e) {
            throw new UdfRuntimeException("UDAF failed to serialize: ", e);
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
            allMethods.get(UDAF_DESERIALIZE_FUNCTION).invoke(udaf, args);
            args[1] = args[0];
            Long curPlace = place;
            stateObjMap.putIfAbsent(curPlace, createAggState());
            args[0] = stateObjMap.get(curPlace);
            allMethods.get(UDAF_MERGE_FUNCTION).invoke(udaf, args);
        } catch (Exception e) {
            throw new UdfRuntimeException("UDAF failed to merge: ", e);
        }
    }

    /**
     * invoke getValue to return finally result.
     */
    public boolean getValue(long row, long place) throws UdfRuntimeException {
        try {
            return storeUdfResult(allMethods.get(UDAF_RESULT_FUNCTION).invoke(udaf, stateObjMap.get((Long) place)),
                    row);
        } catch (Exception e) {
            throw new UdfRuntimeException("UDAF failed to result", e);
        }
    }

    private boolean storeUdfResult(Object obj, long row) throws UdfRuntimeException {
        if (obj == null) {
            // If result is null, return true directly when row == 0 as we have already inserted default value.
            if (UdfUtils.UNSAFE.getLong(null, outputNullPtr) == -1) {
                throw new UdfRuntimeException("UDAF failed to store null data to not null column");
            }
            return true;
        }
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
                long time = UdfUtils.convertToDateV2(obj, retClass);
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
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
                BigInteger data = ((BigDecimal) obj).unscaledValue();
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
            case CHAR:
            case VARCHAR:
            case STRING: {
                long bufferSize = UdfUtils.UNSAFE.getLong(null, outputIntermediateStatePtr);
                byte[] bytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
                long offset = Integer.toUnsignedLong(
                        UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 4L * (row - 1)));
                if (offset + bytes.length > bufferSize) {
                    return false;
                }
                offset += bytes.length;
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 4L * row,
                        Integer.parseUnsignedInt(String.valueOf(offset)));
                UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + offset - bytes.length, bytes.length);
                return true;
            }
            default:
                throw new UdfRuntimeException("Unsupported return type: " + retType);
        }
    }

    private Object[] allocateInputObjects(long row) throws UdfRuntimeException {
        Object[] inputObjects = new Object[argTypes.length];

        for (int i = 0; i < argTypes.length; ++i) {
            // skip the input column of current row is null
            if (UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputNullsPtrs, i)) != -1
                    && UdfUtils.UNSAFE.getByte(null, UdfUtils.getAddressAtOffset(inputNullsPtrs, i) + row) == 1) {
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
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 2L * row);
                    break;
                case INT:
                    inputObjects[i] = UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 4L * row);
                    break;
                case BIGINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    break;
                case FLOAT:
                    inputObjects[i] = UdfUtils.UNSAFE.getFloat(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 4L * row);
                    break;
                case DOUBLE:
                    inputObjects[i] = UdfUtils.UNSAFE.getDouble(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    break;
                case DATE: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    inputObjects[i] = UdfUtils.convertDateToJavaDate(data, argClass[i + 1]);
                    break;
                }
                case DATETIME: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    inputObjects[i] = UdfUtils.convertDateTimeToJavaDateTime(data, argClass[i + 1]);
                    break;
                }
                case DATEV2: {
                    int data = UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 4L * row);
                    inputObjects[i] = UdfUtils.convertDateV2ToJavaDate(data, argClass[i + 1]);
                    break;
                }
                case DATETIMEV2: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    inputObjects[i] = UdfUtils.convertDateTimeV2ToJavaDateTime(data, argClass[i + 1]);
                    break;
                }
                case LARGEINT: {
                    long base = UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                            + 16L * row;
                    byte[] bytes = new byte[16];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);

                    inputObjects[i] = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    break;
                }
                case DECIMALV2: {
                    long base = UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                            + 16L * row;
                    byte[] bytes = new byte[16];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);

                    BigInteger value = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    inputObjects[i] = new BigDecimal(value, 9);
                    break;
                }
                case CHAR:
                case VARCHAR:
                case STRING: {
                    long offset = Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i))
                                    + 4L * row));
                    long numBytes = row == 0 ? offset : offset - Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i)) + 4L * (row
                                    - 1)));
                    long base = row == 0 ? UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.getAddressAtOffset(inputBufferPtrs, i))
                            : UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + offset
                                    - numBytes;
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

    private void init(String jarPath, String udfPath, Type funcRetType, Type... parameterTypes)
            throws UdfRuntimeException {
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
            Class<?> c = Class.forName(udfPath, true, loader);
            Constructor<?> ctor = c.getConstructor();
            udaf = ctor.newInstance();
            Method[] methods = c.getDeclaredMethods();
            int idx = 0;
            for (idx = 0; idx < methods.length; ++idx) {
                signatures.add(methods[idx].toGenericString());
                switch (methods[idx].getName()) {
                    case UDAF_DESTROY_FUNCTION:
                    case UDAF_CREATE_FUNCTION:
                    case UDAF_MERGE_FUNCTION:
                    case UDAF_SERIALIZE_FUNCTION:
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
            sb.append("Unable to find evaluate function with the correct signature: ").append(udfPath + ".evaluate(")
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
