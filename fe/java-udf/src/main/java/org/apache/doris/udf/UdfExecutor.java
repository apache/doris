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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

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
    // inputArgs_[i] is either inputObjects_[i] or null
    private Object[] inputArgs;

    private long outputOffset;
    private long rowIdx;

    private final long batchSizePtr;

    // Data types that are supported as return or argument types in Java UDFs.
    public enum JavaUdfDataType {
        INVALID_TYPE("INVALID_TYPE", TPrimitiveType.INVALID_TYPE, 0),
        BOOLEAN("BOOLEAN", TPrimitiveType.BOOLEAN, 1),
        TINYINT("TINYINT", TPrimitiveType.TINYINT, 1),
        SMALLINT("SMALLINT", TPrimitiveType.SMALLINT, 2),
        INT("INT", TPrimitiveType.INT, 4),
        BIGINT("BIGINT", TPrimitiveType.BIGINT, 8),
        FLOAT("FLOAT", TPrimitiveType.FLOAT, 4),
        DOUBLE("DOUBLE", TPrimitiveType.DOUBLE, 8),
        CHAR("CHAR", TPrimitiveType.CHAR, 0),
        VARCHAR("VARCHAR", TPrimitiveType.VARCHAR, 0),
        STRING("STRING", TPrimitiveType.STRING, 0);

        private final String description_;
        private final TPrimitiveType thriftType_;
        private final int len_;

        JavaUdfDataType(String description, TPrimitiveType thriftType, int len) {
            description_ = description;
            thriftType_ = thriftType;
            len_ = len;
        }

        @Override
        public String toString() {
            return description_;
        }

        public TPrimitiveType getPrimitiveType() {
            return thriftType_;
        }

        public int getLen() {
            return len_;
        }

        public static JavaUdfDataType getType(Class<?> c) {
            if (c == boolean.class || c == Boolean.class) {
                return JavaUdfDataType.BOOLEAN;
            } else if (c == byte.class || c == Byte.class) {
                return JavaUdfDataType.TINYINT;
            } else if (c == short.class || c == Short.class) {
                return JavaUdfDataType.SMALLINT;
            } else if (c == int.class || c == Integer.class) {
                return JavaUdfDataType.INT;
            } else if (c == long.class || c == Long.class) {
                return JavaUdfDataType.BIGINT;
            } else if (c == float.class || c == Float.class) {
                return JavaUdfDataType.FLOAT;
            } else if (c == double.class || c == Double.class) {
                return JavaUdfDataType.DOUBLE;
            } else if (c == char.class || c == Character.class) {
                return JavaUdfDataType.CHAR;
            } else if (c == String.class) {
                return JavaUdfDataType.STRING;
            }
            return JavaUdfDataType.INVALID_TYPE;
        }

        public static boolean isSupported(Type t) {
            for (JavaUdfDataType javaType : JavaUdfDataType.values()) {
                if (javaType == JavaUdfDataType.INVALID_TYPE) continue;
                if (javaType.getPrimitiveType() == t.getPrimitiveType().toThrift()) {
                    return true;
                }
            }
            return false;
        }
    }

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
        int batch_size = UdfUtils.UNSAFE.getInt(null, batchSizePtr);
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
            for (; rowIdx < batch_size; rowIdx++) {
                allocateInputObjects(rowIdx);
                for (int i = 0; i < argTypes.length; ++i) {
                    // Currently, -1 indicates this column is not nullable. So input argument is
                    // null iff inputNullsPtrs_ != -1 and nullCol[row_idx] != 0.
                    if (UdfUtils.UNSAFE.getLong(null,
                            UdfUtils.getAddressAtOffset(inputNullsPtrs, i)) == -1 ||
                            UdfUtils.UNSAFE.getByte(null, UdfUtils.UNSAFE.getLong(null,
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
                UdfUtils.UNSAFE.putLong(null, outputIntermediateStatePtr + 8, batch_size);
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

    // Sets the result object 'obj' into the outputBufferPtr_ and outputNullPtr_
    private boolean storeUdfResult(Object obj, long row) throws UdfRuntimeException {
        if (obj == null) {
            assert (UdfUtils.UNSAFE.getLong(null, outputNullPtr) != -1);
            UdfUtils.UNSAFE.putByte(null, UdfUtils.UNSAFE.getLong(null, outputNullPtr) + row, (byte) 1);
            if (retType.equals(JavaUdfDataType.STRING)) {
                long bufferSize = UdfUtils.UNSAFE.getLong(null, outputIntermediateStatePtr);
                if (outputOffset + 1 > bufferSize) {
                    return false;
                }
                outputOffset += 1;
                UdfUtils.UNSAFE.putChar(null, UdfUtils.UNSAFE.getLong(null, outputBufferPtr) +
                    outputOffset - 1, UdfUtils.END_OF_STRING);
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) +
                        4L * row, Integer.parseUnsignedInt(String.valueOf(outputOffset)));
            }
            return true;
        }
        if (UdfUtils.UNSAFE.getLong(null, outputNullPtr) != -1) {
            UdfUtils.UNSAFE.putByte(UdfUtils.UNSAFE.getLong(null, outputNullPtr) + row, (byte) 0);
        }
        switch (retType) {
            case BOOLEAN: {
                boolean val = (boolean) obj;
                UdfUtils.UNSAFE.putByte(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), val ? (byte) 1 : 0);
                return true;
            }
            case TINYINT: {
                UdfUtils.UNSAFE.putByte(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), (byte) obj);
                return true;
            }
            case SMALLINT: {
                UdfUtils.UNSAFE.putShort(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), (short) obj);
                return true;
            }
            case INT: {
                UdfUtils.UNSAFE.putInt(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), (int) obj);
                return true;
            }
            case BIGINT: {
                UdfUtils.UNSAFE.putLong(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), (long) obj);
                return true;
            }
            case FLOAT: {
                UdfUtils.UNSAFE.putFloat(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), (float) obj);
                return true;
            }
            case DOUBLE: {
                UdfUtils.UNSAFE.putDouble(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) + row * retType.getLen(), (double) obj);
                return true;
            }
            case CHAR:
            case VARCHAR:
            case STRING:
                long bufferSize = UdfUtils.UNSAFE.getLong(null, outputIntermediateStatePtr);
                byte[] bytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
                if (outputOffset + bytes.length + 1 > bufferSize) {
                    return false;
                }
                outputOffset += (bytes.length + 1);
                UdfUtils.UNSAFE.putChar(UdfUtils.UNSAFE.getLong(null, outputBufferPtr) +
                    outputOffset - 1, UdfUtils.END_OF_STRING);
                UdfUtils.UNSAFE.putInt(null, UdfUtils.UNSAFE.getLong(null, outputOffsetsPtr) + 4L * row,
                        Integer.parseUnsignedInt(String.valueOf(outputOffset)));
                UdfUtils.copyMemory(bytes, UdfUtils.BYTE_ARRAY_OFFSET, null,
                        UdfUtils.UNSAFE.getLong(null, outputBufferPtr) +
                            outputOffset - bytes.length - 1, bytes.length);
                return true;
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
                    inputObjects[i] = UdfUtils.UNSAFE.getBoolean(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + row);
                    break;
                case TINYINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getByte(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + row);
                    break;
                case SMALLINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getShort(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + 2L * row);
                    break;
                case INT:
                    inputObjects[i] = UdfUtils.UNSAFE.getInt(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + 4L * row);
                    break;
                case BIGINT:
                    inputObjects[i] = UdfUtils.UNSAFE.getLong(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + 8L * row);
                    break;
                case FLOAT:
                    inputObjects[i] = UdfUtils.UNSAFE.getFloat(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + 4L * row);
                    break;
                case DOUBLE:
                    inputObjects[i] = UdfUtils.UNSAFE.getDouble(null, UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(
                        inputBufferPtrs, i)) + 8L * row);
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                    long offset = Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null,
                                    UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i)) + 4L * row));
                    long numBytes = row == 0 ? offset - 1 : offset - Integer.toUnsignedLong(UdfUtils.UNSAFE.getInt(null,
                            UdfUtils.UNSAFE.getLong(null,
                                    UdfUtils.getAddressAtOffset(inputOffsetsPtrs, i)) + 4L * (row - 1))) - 1;
                    long base = row == 0 ? UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) :
                            UdfUtils.UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs, i)) + offset - numBytes - 1;
                    byte[] bytes = new byte[(int) numBytes];
                    UdfUtils.copyMemory(null, base, bytes, UdfUtils.BYTE_ARRAY_OFFSET, numBytes);
                    inputObjects[i] = new String(bytes, StandardCharsets.UTF_8);
                    break;
                default:
                    throw new UdfRuntimeException("Unsupported argument type: " + argTypes[i]);
            }
        }
    }

    private URLClassLoader getClassLoader(String jarPath) throws MalformedURLException {
        URL url = new File(jarPath).toURI().toURL();
        return URLClassLoader.newInstance(new URL[]{url}, getClass().getClassLoader());
    }

    /**
     * Sets the return type of a Java UDF. Returns true if the return type is compatible
     * with the return type from the function definition. Throws an UdfRuntimeException
     * if the return type is not supported.
     */
    private boolean setReturnType(Type retType, Class<?> udfReturnType)
            throws InternalException {
        if (!JavaUdfDataType.isSupported(retType)) {
            throw new InternalException("Unsupported return type: " + retType.toSql());
        }
        JavaUdfDataType javaType = JavaUdfDataType.getType(udfReturnType);
        // Check if the evaluate method return type is compatible with the return type from
        // the function definition. This happens when both of them map to the same primitive
        // type.
        if (retType.getPrimitiveType().toThrift() != javaType.getPrimitiveType()) {
            return false;
        }
        this.retType = javaType;
        return true;
    }

    /**
     * Sets the argument types of a Java UDF. Returns true if the argument types specified
     * in the UDF are compatible with the argument types of the evaluate() function loaded
     * from the associated JAR file.
     */
    private boolean setArgTypes(Type[] parameterTypes, Class<?>[] udfArgTypes) {
        Preconditions.checkNotNull(argTypes);
        for (int i = 0; i < udfArgTypes.length; ++i) {
            argTypes[i] = JavaUdfDataType.getType(udfArgTypes[i]);
            if (argTypes[i].getPrimitiveType()
                    != parameterTypes[i].getPrimitiveType().toThrift()) {
                return false;
            }
        }
        return true;
    }

    private void init(String jarPath, String udfPath,
                      Type retType, Type... parameterTypes) throws UdfRuntimeException {
        ArrayList<String> signatures = Lists.newArrayList();
        try {
            LOG.debug("Loading UDF '" + udfPath + "' from " + jarPath);
            ClassLoader loader;
            if (jarPath != null) {
                // Save for cleanup.
                classLoader = getClassLoader(jarPath);
                loader = classLoader;
            } else {
                loader = ClassLoader.getSystemClassLoader();
            }
            Class<?> c = Class.forName(udfPath, true, loader);
            Constructor<?> ctor = c.getConstructor();
            udf = ctor.newInstance();
            argTypes = new JavaUdfDataType[parameterTypes.length];
            Method[] methods = c.getMethods();
            for (Method m : methods) {
                // By convention, the udf must contain the function "evaluate"
                if (!m.getName().equals(UDF_FUNCTION_NAME)) continue;
                signatures.add(m.toGenericString());
                Class<?>[] methodTypes = m.getParameterTypes();

                // Try to match the arguments
                if (methodTypes.length != parameterTypes.length) continue;
                method = m;
                if (methodTypes.length == 0 && parameterTypes.length == 0) {
                    // Special case where the UDF doesn't take any input args
                    if (!setReturnType(retType, m.getReturnType())) continue;
                    LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
                    return;
                }
                if (!setReturnType(retType, m.getReturnType())) continue;
                if (!setArgTypes(parameterTypes, methodTypes)) continue;
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
