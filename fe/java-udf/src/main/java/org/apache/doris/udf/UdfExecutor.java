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

import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;

public class UdfExecutor {
    private static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    public static final Unsafe UNSAFE;

    static {
        UNSAFE = (Unsafe) AccessController.doPrivileged(
                (PrivilegedAction<Object>) () -> {
                    try {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new Error();
                    }
                });
    }

    // By convention, the function in the class must be called evaluate()
    public static final String UDF_FUNCTION_NAME = "evaluate";

    // Object to deserialize ctor params from BE.
    private final static TBinaryProtocol.Factory PROTOCOL_FACTORY =
            new TBinaryProtocol.Factory();

    private Object udf_;
    // setup by init() and cleared by close()
    private Method method_;
    // setup by init() and cleared by close()
    private URLClassLoader classLoader_;

    // Return and argument types of the function inferred from the udf method signature.
    // The JavaUdfDataType enum maps it to corresponding primitive type.
    private JavaUdfDataType[] argTypes_;
    private JavaUdfDataType retType_;

    // Input buffer from the backend. This is valid for the duration of an evaluate() call.
    // These buffers are allocated in the BE.
    private final long inputBufferPtrs_;
    private final long inputNullsPtrs_;

    // Output buffer to return non-string values. These buffers are allocated in the BE.
    private final long outputBufferPtr_;
    private final long outputNullPtr_;

    // Pre-constructed input objects for the UDF. This minimizes object creation overhead
    // as these objects are reused across calls to evaluate().
    private Object[] inputObjects_;
    // inputArgs_[i] is either inputObjects_[i] or null
    private Object[] inputArgs_;

    private final long batch_size_ptr_;

    // Data types that are supported as return or argument types in Java UDFs.
    public enum JavaUdfDataType {
        INVALID_TYPE("INVALID_TYPE", TPrimitiveType.INVALID_TYPE, 0),
        BOOLEAN("BOOLEAN", TPrimitiveType.BOOLEAN, 1),
        TINYINT("TINYINT", TPrimitiveType.TINYINT, 1),
        SMALLINT("SMALLINT", TPrimitiveType.SMALLINT, 2),
        INT("INT", TPrimitiveType.INT, 4),
        BIGINT("BIGINT", TPrimitiveType.BIGINT, 8),
        FLOAT("FLOAT", TPrimitiveType.FLOAT, 4),
        DOUBLE("DOUBLE", TPrimitiveType.DOUBLE, 8);

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
        batch_size_ptr_ = request.batch_size_ptr;
        inputBufferPtrs_ = request.input_buffer_ptrs;
        inputNullsPtrs_ = request.input_nulls_ptrs;
        outputBufferPtr_ = request.output_buffer_ptr;
        outputNullPtr_ = request.output_null_ptr;

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
        if (classLoader_ != null) {
            try {
                classLoader_.close();
            } catch (IOException e) {
                // Log and ignore.
                LOG.debug("Error closing the URLClassloader.", e);
            }
        }
        // We are now un-usable (because the class loader has been
        // closed), so null out method_ and classLoader_.
        method_ = null;
        classLoader_ = null;
    }

    /**
     * evaluate function called by the backend. The inputs to the UDF have
     * been serialized to 'input'
     */
    public void evaluate() throws UdfRuntimeException {
        try {
            int batch_size = UNSAFE.getInt(null, batch_size_ptr_);
            for (int row = 0; row < batch_size; row++) {
                allocateInputObjects(row);
                for (int i = 0; i < argTypes_.length; ++i) {
                    if (UNSAFE.getByte(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputNullsPtrs_, i)) + row * 1L) == 0) {
                        inputArgs_[i] = inputObjects_[i];
                    } else {
                        inputArgs_[i] = null;
                    }
                }
                storeUdfResult(evaluate(inputArgs_), row);
            }
        } catch (Exception e) {
            throw new UdfRuntimeException("UDF::evaluate() ran into a problem.", e);
        }
    }

    /**
     * Evaluates the UDF with 'args' as the input to the UDF.
     */
    private Object evaluate(Object... args) throws UdfRuntimeException {
        try {
            return method_.invoke(udf_, args);
        } catch (Exception e) {
            throw new UdfRuntimeException("UDF failed to evaluate", e);
        }
    }

    public Method getMethod() {
        return method_;
    }

    // Sets the result object 'obj' into the outputBufferPtr_ and outputNullPtr_
    private void storeUdfResult(Object obj, int row) throws UdfRuntimeException {
        if (obj == null) {
            UNSAFE.putByte(null, UNSAFE.getLong(null, outputNullPtr_) + row * 1L, (byte) 1);
            return;
        }
        UNSAFE.putByte(UNSAFE.getLong(null, outputNullPtr_) + row * 1L, (byte) 0);
        switch (retType_) {
            case BOOLEAN: {
                boolean val = (boolean) obj;
                UNSAFE.putByte(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), val ? (byte) 1 : 0);
                return;
            }
            case TINYINT: {
                UNSAFE.putByte(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), (byte) obj);
                return;
            }
            case SMALLINT: {
                UNSAFE.putShort(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), (short) obj);
                return;
            }
            case INT: {
                UNSAFE.putInt(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), (int) obj);
                return;
            }
            case BIGINT: {
                UNSAFE.putLong(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), (long) obj);
                return;
            }
            case FLOAT: {
                UNSAFE.putFloat(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), (float) obj);
                return;
            }
            case DOUBLE: {
                UNSAFE.putDouble(UNSAFE.getLong(null, outputBufferPtr_) + row * retType_.getLen(), (double) obj);
                return;
            }
            default:
                throw new UdfRuntimeException("Unsupported return type: " + retType_);
        }
    }

    // Preallocate the input objects that will be passed to the underlying UDF.
    // These objects are allocated once and reused across calls to evaluate()
    private void allocateInputObjects(int row) throws UdfRuntimeException {
        inputObjects_ = new Object[argTypes_.length];
        inputArgs_ = new Object[argTypes_.length];

        for (int i = 0; i < argTypes_.length; ++i) {
            switch (argTypes_[i]) {
                case BOOLEAN:
                    inputObjects_[i] = UNSAFE.getBoolean(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 1L * row);
                    break;
                case TINYINT:
                    inputObjects_[i] = UNSAFE.getByte(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 1L * row);
                    break;
                case SMALLINT:
                    inputObjects_[i] = UNSAFE.getShort(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 2L * row);
                    break;
                case INT:
                    inputObjects_[i] = UNSAFE.getInt(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 4L * row);
                    break;
                case BIGINT:
                    inputObjects_[i] = UNSAFE.getLong(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 8L * row);
                    break;
                case FLOAT:
                    inputObjects_[i] = UNSAFE.getFloat(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 4L * row);
                    break;
                case DOUBLE:
                    inputObjects_[i] = UNSAFE.getDouble(null, UNSAFE.getLong(null, UdfUtils.getAddressAtOffset(inputBufferPtrs_, i)) + 8L * row);
                    break;
                default:
                    throw new UdfRuntimeException("Unsupported argument type: " + argTypes_[i]);
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
        retType_ = javaType;
        return true;
    }

    /**
     * Sets the argument types of a Java UDF. Returns true if the argument types specified
     * in the UDF are compatible with the argument types of the evaluate() function loaded
     * from the associated JAR file.
     */
    private boolean setArgTypes(Type[] parameterTypes, Class<?>[] udfArgTypes) {
        Preconditions.checkNotNull(argTypes_);
        for (int i = 0; i < udfArgTypes.length; ++i) {
            argTypes_[i] = JavaUdfDataType.getType(udfArgTypes[i]);
            if (argTypes_[i].getPrimitiveType()
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
                classLoader_ = getClassLoader(jarPath);
                loader = classLoader_;
            } else {
                loader = ClassLoader.getSystemClassLoader();
            }
            Class<?> c = Class.forName(udfPath, true, loader);
            Constructor<?> ctor = c.getConstructor();
            udf_ = ctor.newInstance();
            argTypes_ = new JavaUdfDataType[parameterTypes.length];
            Method[] methods = c.getMethods();
            for (Method m : methods) {
                // By convention, the udf must contain the function "evaluate"
                if (!m.getName().equals(UDF_FUNCTION_NAME)) continue;
                signatures.add(m.toGenericString());
                Class<?>[] methodTypes = m.getParameterTypes();

                // Try to match the arguments
                if (methodTypes.length != parameterTypes.length) continue;
                method_ = m;
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
