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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;

public class UdfExecutor extends BaseExecutor {
    // private static final java.util.logging.Logger LOG =
    // Logger.getLogger(UdfExecutor.class);
    public static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    // setup by init() and cleared by close()
    private Method method;

    private int evaluateIndex;

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
                keyStrOffsetAddr, argTypes[argIdx].getKeyScale());
        Object[] valueCol = convertMapArg(valueType, argIdx, isNullable, 0, numRows, nullMapAddr, offsetsAddr,
                valueNestedNullMapAddr, valueDataAddr,
                valueStrOffsetAddr, argTypes[argIdx].getValueScale());
        return buildHashMap(keyType, valueType, keyCol, valueCol);
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
            LOG.info("evaluate exception: " + debugString());
            LOG.info("evaluate(int numRows, Object[] column) Exception: " + e.toString());
            throw new UdfRuntimeException("UDF failed to evaluate", e);
        }
    }

    public void copyBatchBasicResult(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long resColumnAddr, long strOffsetAddr) {
        copyBatchBasicResultImpl(isNullable, numRows, result, nullMapAddr, resColumnAddr, strOffsetAddr, getMethod());
    }

    public void copyBatchArrayResult(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long offsetsAddr, long nestedNullMapAddr, long dataAddr, long strOffsetAddr) {
        Preconditions.checkState(result.length == numRows,
                "copyBatchArrayResult result size should equal;");
        copyBatchArrayResultImpl(isNullable, numRows, result, nullMapAddr, offsetsAddr, nestedNullMapAddr, dataAddr,
                strOffsetAddr, retType.getItemType().getPrimitiveType(), retType.getScale());
    }

    public void copyBatchMapResult(boolean isNullable, int numRows, Object[] result, long nullMapAddr,
            long offsetsAddr, long keyNsestedNullMapAddr, long keyDataAddr, long keyStrOffsetAddr,
            long valueNsestedNullMapAddr, long valueDataAddr, long valueStrOffsetAddr) {
        Preconditions.checkState(result.length == numRows,
                "copyBatchMapResult result size should equal;");
        PrimitiveType keyType = retType.getKeyType().getPrimitiveType();
        PrimitiveType valueType = retType.getValueType().getPrimitiveType();
        Object[] keyCol = new Object[result.length];
        Object[] valueCol = new Object[result.length];
        buildArrayListFromHashMap(result, keyType, valueType, keyCol, valueCol);

        copyBatchArrayResultImpl(isNullable, numRows, valueCol, nullMapAddr, offsetsAddr, valueNsestedNullMapAddr,
                valueDataAddr,
                valueStrOffsetAddr, valueType, retType.getKeyScale());
        copyBatchArrayResultImpl(isNullable, numRows, keyCol, nullMapAddr, offsetsAddr, keyNsestedNullMapAddr,
                keyDataAddr,
                keyStrOffsetAddr, keyType, retType.getValueScale());
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

    // Preallocate the input objects that will be passed to the underlying UDF.
    // These objects are allocated once and reused across calls to evaluate()
    @Override
    protected void init(TJavaUdfExecutorCtorParams request, String jarPath, Type funcRetType,
            Type... parameterTypes) throws UdfRuntimeException {
        String className = request.fn.scalar_fn.symbol;
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
                Type keyType = retType.getKeyType();
                Type valueType = retType.getValueType();
                Pair<Boolean, JavaUdfDataType[]> inputType = UdfUtils.setArgTypes(parameterTypes, argClass, false);
                if (!inputType.first) {
                    continue;
                } else {
                    argTypes = inputType.second;
                }
                LOG.debug("Loaded UDF '" + className + "' from " + jarPath);
                retType.setKeyType(keyType);
                retType.setValueType(valueType);
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
