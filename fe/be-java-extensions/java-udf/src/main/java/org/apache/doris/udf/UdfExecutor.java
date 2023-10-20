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
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.ColumnValueConverter;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.esotericsoftware.reflectasm.MethodAccess;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UdfExecutor extends BaseExecutor {
    public static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    // setup by init() and cleared by close()
    private Method method;

    private int evaluateIndex;

    private VectorTable outputTable = null;

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
        // inputTable is released by c++, only release outputTable
        if (outputTable != null) {
            outputTable.close();
        }
        // We are now un-usable (because the class loader has been
        // closed), so null out method_ and classLoader_.
        method = null;
        super.close();
    }

    private Map<Integer, ColumnValueConverter> getInputConverters(int numColumns) {
        Map<Integer, ColumnValueConverter> converters = new HashMap<>();
        for (int j = 0; j < numColumns; ++j) {
            ColumnValueConverter converter = getInputConverter(argTypes[j].getPrimitiveType(), argClass[j]);
            if (converter != null) {
                converters.put(j, converter);
            }
        }
        return converters;
    }

    private ColumnValueConverter getOutputConverter() {
        return getOutputConverter(retType.getPrimitiveType(), method.getReturnType());
    }

    public long evaluate(Map<String, String> inputParams, Map<String, String> outputParams) throws UdfRuntimeException {
        try {
            VectorTable inputTable = VectorTable.createReadableTable(inputParams);
            int numRows = inputTable.getNumRows();
            int numColumns = inputTable.getNumColumns();
            if (outputTable != null) {
                outputTable.close();
            }
            outputTable = VectorTable.createWritableTable(outputParams, numRows);

            // If the return type is primitive, we can't cast the array of primitive type as array of Object,
            // so we have to new its wrapped Object.
            Object[] result = outputTable.getColumnType(0).isPrimitive()
                    ? outputTable.getColumn(0).newObjectContainerArray(numRows)
                    : (Object[]) Array.newInstance(method.getReturnType(), numRows);
            Object[][] inputs = inputTable.getMaterializedData(getInputConverters(numColumns));
            Object[] parameters = new Object[numColumns];
            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < numColumns; ++j) {
                    parameters[j] = inputs[j][i];
                }
                result[i] = methodAccess.invoke(udf, evaluateIndex, parameters);
            }
            boolean isNullable = Boolean.parseBoolean(outputParams.getOrDefault("is_nullable", "true"));
            outputTable.appendData(0, result, getOutputConverter(), isNullable);
            return outputTable.getMetaAddress();
        } catch (Exception e) {
            LOG.warn("evaluate exception: " + debugString(), e);
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
                    .append(className)
                    .append(".evaluate(")
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
