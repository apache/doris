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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class UdfExecutor {
    private static final Logger LOG = Logger.getLogger(UdfExecutor.class);

    // By convention, the function in the class must be called evaluate()
    public static final String UDF_FUNCTION_NAME = "evaluate";

    // Object to deserialize ctor params from BE.
    private static final TBinaryProtocol.Factory PROTOCOL_FACTORY =
            new TBinaryProtocol.Factory();

    private Object udf;
    // setup by init() and cleared by close()
    private Method method;
    // setup by init() and cleared by close()
    private URLClassLoader classLoader;

    // Return and argument types of the function inferred from the udf method signature.
    // The JavaUdfDataType enum maps it to corresponding primitive type.
    private JavaUdfDataType[] argTypes;
    private JavaUdfDataType retType;

    // Input buffer from the backend. This is valid for the duration of an evaluate() call.
    // These buffers are allocated in the BE.
    private final long inputBufferPtrs;
    private final long inputNullsPtrs;
    private final long inputOffsetsPtrs;

    // Output buffer to return non-string values. These buffers are allocated in the BE.
    private final long outputBufferPtr;
    private final long outputNullPtr;
    private final long outputOffsetsPtr;
    private final long outputIntermediateStatePtr;

    // Pre-constructed input objects for the UDF. This minimizes object creation overhead
    // as these objects are reused across calls to evaluate().
    private Object[] inputObjects;
    // inputArgs_[i] is either inputObjects[i] or null
    private Object[] inputArgs;

    private long outputOffset;
    private long rowIdx;

    private final long batchSizePtr;
    private Class[] argClass;

    /**
     * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
     * the backend.
     */
    public UdfExecutor(byte[] thriftParams) throws Exception {
        TJavaUdfExecutorCtorParams request = new TJavaUdfExecutorCtorParams();
        TDeserializer deserializer = new TDeserializer(PROTOCOL_FACTORY);
        try {
            deserializer.deserialize(request, thriftParams);
        } catch (TException e) {
            throw new InternalException(e.getMessage());
        }
        String className = request.fn.scalar_fn.symbol;
        String jarFile = request.location;
        Type retType = UdfUtils.fromThrift(request.fn.ret_type, 0).first;
        Type[] parameterTypes = new Type[request.fn.arg_types.size()];
        for (int i = 0; i < request.fn.arg_types.size(); ++i) {
            parameterTypes[i] = Type.fromThrift(request.fn.arg_types.get(i));
        }
        batchSizePtr = request.batch_size_ptr;
        inputBufferPtrs = request.input_buffer_ptrs;
        inputNullsPtrs = request.input_nulls_ptrs;
        inputOffsetsPtrs = request.input_offsets_ptrs;

        outputBufferPtr = request.output_buffer_ptr;
        outputNullPtr = request.output_null_ptr;
        outputOffsetsPtr = request.output_offsets_ptr;
        outputIntermediateStatePtr = request.output_intermediate_state_ptr;

        outputOffset = 0L;
        rowIdx = 0L;

        init(jarFile, className, retType, parameterTypes);
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

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
        method = null;
        classLoader = null;
    }

    /**
     * evaluate function called by the backend. The inputs to the UDF have
     * been serialized to 'input'
     */
    public void evaluate() throws UdfRuntimeException {
        int batchSize = UdfUtils.UNSAFE.getInt(null, batchSizePtr);
        try {
            if (retType.equals(JavaUdfDataType.STRING) || retType.equals(JavaUdfDataType.VARCHAR)
                    || retType.equals(JavaUdfDataType.CHAR)) {
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
                allocateInputObjects(rowIdx);
                for (int i = 0; i < argTypes.length; ++i) {
                    // Currently, -1 indicates this column is not nullable. So input argument is
                    // null iff inputNullsPtrs_ != -1 and nullCol[row_idx] != 0.
                    if (UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.getAddressAtOffset(inputNullsPtrs, i)) == -1
                            || UdfUtils.UNSAFE.getByte(null, UdfUtils.UNSAFE.getLong(null,
                                    UdfUtils.getAddressAtOffset(inputNullsPtrs, i)) + rowIdx) == 0) {
                        inputArgs[i] = inputObjects[i];
                    } else {
                        inputArgs[i] = null;
                    }
                }
                // `storeUdfResult` is called to store udf result to output column. If true
                // is returned, current value is stored successfully. Otherwise, current result is
                // not processed successfully (e.g. current output buffer is not large enough) so
                // we break this loop directly.
                if (!storeUdfResult(evaluate(inputArgs), rowIdx)) {
                    UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, rowIdx);
                    return;
                }
            }
        } catch (Exception e) {
            if (retType.equals(JavaUdfDataType.STRING)) {
                UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, batchSize);
            }
            throw new UdfRuntimeException("UDF::evaluate() ran into a problem.", e);
        }
        if (retType.equals(JavaUdfDataType.STRING)) {
            UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, rowIdx);
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
    private boolean storeUdfResult(Object obj, long row) throws UdfRuntimeException {
        if (obj == null) {
            if (UdfUtils.UNSAFE.getLong(null, outputNullPtr) == -1) {
                throw new UdfRuntimeException("UDF failed to store null data to not null column");
            }
            UdfUtils.UNSAFE.putByte(null, UdfUtils.UNSAFE.getLong(null, outputNullPtr) + row, (byte) 1);
            if (retType.equals(JavaUdfDataType.STRING)) {
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr)
                        + 4L * row, Integer.parseUnsignedInt(String.valueOf(outputOffset)));
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
                long time = UdfUtils.convertToDate(obj, method.getReturnType());
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case DATETIME: {
                long time = UdfUtils.convertToDateTime(obj, method.getReturnType());
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case DATEV2: {
                int time = UdfUtils.convertToDateV2(obj, method.getReturnType());
                UdfUtils.UNSAFE.putInt(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), time);
                return true;
            }
            case DATETIMEV2: {
                long time = UdfUtils.convertToDateTimeV2(obj, method.getReturnType());
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
                if (outputOffset + bytes.length > bufferSize) {
                    return false;
                }
                outputOffset += bytes.length;
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 4L * row,
                        Integer.parseUnsignedInt(String.valueOf(outputOffset)));
                UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + outputOffset - bytes.length, bytes.length);
                return true;
            }
            default:
                throw new UdfRuntimeException("Unsupported return type: " + retType);
        }
    }

    // Preallocate the input objects that will be passed to the underlying UDF.
    // These objects are allocated once and reused across calls to evaluate()
    private void allocateInputObjects(long row) throws UdfRuntimeException {
        inputObjects = new Object[argTypes.length];
        inputArgs = new Object[argTypes.length];

        for (int i = 0; i < argTypes.length; ++i) {
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
                    inputObjects[i] = UdfUtils.convertDateToJavaDate(data, argClass[i]);
                    break;
                }
                case DATETIME: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    inputObjects[i] = UdfUtils.convertDateTimeToJavaDateTime(data, argClass[i]);
                    break;
                }
                case DATEV2: {
                    int data = UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 4L * row);
                    inputObjects[i] = UdfUtils.convertDateV2ToJavaDate(data, argClass[i]);
                    break;
                }
                case DATETIMEV2: {
                    long data = UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 8L * row);
                    inputObjects[i] = UdfUtils.convertDateTimeV2ToJavaDateTime(data, argClass[i]);
                    break;
                }
                case LARGEINT: {
                    long base =
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 16L * row;
                    byte[] bytes = new byte[16];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);

                    inputObjects[i] = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    break;
                }
                case DECIMALV2: {
                    long base =
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + 16L * row;
                    byte[] bytes = new byte[16];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, 16);

                    BigInteger value = new BigInteger(UdfUtils.convertByteOrder(bytes));
                    inputObjects[i] = new BigDecimal(value, 9);
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
    }

    private void init(String jarPath, String udfPath, Type funcRetType, Type... parameterTypes)
            throws UdfRuntimeException {
        ArrayList<String> signatures = Lists.newArrayList();
        try {
            LOG.debug("Loading UDF '" + udfPath + "' from " + jarPath);
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
            Class<?> c = Class.forName(udfPath, true, loader);
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
                    LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
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
                LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
                return;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Unable to find evaluate function with the correct signature: ")
                    .append(udfPath + ".evaluate(")
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
