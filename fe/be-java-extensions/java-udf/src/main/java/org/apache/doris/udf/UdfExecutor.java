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
import org.apache.doris.common.exception.InternalException;
import org.apache.doris.common.exception.UdfRuntimeException;
import org.apache.doris.common.jni.utils.JavaUdfDataType;
import org.apache.doris.common.jni.utils.UdfClassCache;
import org.apache.doris.common.jni.utils.UdfUtils;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TJavaUdfExecutorCtorParams;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;

public class UdfExecutor extends BaseExecutor {
    public static final Logger LOG = Logger.getLogger(UdfExecutor.class);
    private static final String UDF_PREPARE_FUNCTION_NAME = "prepare";
    private static final String UDF_FUNCTION_NAME = "evaluate";

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
        if (!isStaticLoad) {
            super.close();
        } else if (outputTable != null) {
            outputTable.close();
        }
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
                    : (Object[]) Array.newInstance(objCache.retClass, numRows);
            Object[][] inputs = inputTable.getMaterializedData(getInputConverters(numColumns, false));
            Object[] parameters = new Object[numColumns];
            for (int i = 0; i < numRows; ++i) {
                for (int j = 0; j < numColumns; ++j) {
                    int row = inputTable.isConstColumn(j) ? 0 : i;
                    parameters[j] = inputs[j][row];
                }
                result[i] = objCache.methodAccess.invoke(udf, objCache.methodIndex, parameters);
            }
            boolean isNullable = Boolean.parseBoolean(outputParams.getOrDefault("is_nullable", "true"));
            outputTable.appendData(0, result, getOutputConverter(), isNullable);
            return outputTable.getMetaAddress();
        } catch (Exception e) {
            LOG.warn("evaluate exception: " + debugString(), e);
            throw new UdfRuntimeException("UDF failed to evaluate", e);
        }
    }

    private Method findPrepareMethod(Method[] methods) {
        for (Method method : methods) {
            if (method.getName().equals(UDF_PREPARE_FUNCTION_NAME) && method.getReturnType().equals(void.class)
                    && method.getParameterCount() == 0) {
                return method;
            }
        }
        return null; // Method not found
    }

    // Preallocate the input objects that will be passed to the underlying UDF.
    // These objects are allocated once and reused across calls to evaluate()
    @Override
    protected void init(TJavaUdfExecutorCtorParams request, String jarPath, Type funcRetType,
            Type... parameterTypes) throws UdfRuntimeException {
        className = fn.scalar_fn.symbol;
        super.init(request, jarPath, funcRetType, parameterTypes);
        Method prepareMethod = objCache.allMethods.get(UDF_PREPARE_FUNCTION_NAME);
        if (prepareMethod != null) {
            try {
                prepareMethod.invoke(udf);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new UdfRuntimeException("Unable to call UDF prepare function.", e);
            }
        }
    }

    @Override
    protected void checkAndCacheUdfClass(UdfClassCache cache, Type funcRetType, Type... parameterTypes)
            throws InternalException, UdfRuntimeException {
        ArrayList<String> signatures = Lists.newArrayList();
        Class<?> c = cache.udfClass;
        Method[] methods = c.getMethods();
        Method prepareMethod = findPrepareMethod(methods);
        if (prepareMethod != null) {
            cache.allMethods.put(UDF_PREPARE_FUNCTION_NAME, prepareMethod);
        }
        for (Method m : methods) {
            // By convention, the udf must contain the function "evaluate"
            if (!m.getName().equals(UDF_FUNCTION_NAME)) {
                continue;
            }
            signatures.add(m.toGenericString());
            cache.argClass = m.getParameterTypes();

            // Try to match the arguments
            if (cache.argClass.length != parameterTypes.length) {
                continue;
            }
            cache.allMethods.put(UDF_FUNCTION_NAME, m);
            cache.methodIndex = cache.methodAccess.getIndex(UDF_FUNCTION_NAME, cache.argClass);
            Pair<Boolean, JavaUdfDataType> returnType;
            cache.retClass = m.getReturnType();
            if (cache.argClass.length == 0 && parameterTypes.length == 0) {
                // Special case where the UDF doesn't take any input args
                returnType = UdfUtils.setReturnType(funcRetType, m.getReturnType());
                if (!returnType.first) {
                    continue;
                } else {
                    cache.retType = returnType.second;
                }
                cache.argTypes = new JavaUdfDataType[0];
                return;
            }
            returnType = UdfUtils.setReturnType(funcRetType, m.getReturnType());
            if (!returnType.first) {
                continue;
            } else {
                cache.retType = returnType.second;
            }
            Pair<Boolean, JavaUdfDataType[]> inputType = UdfUtils.setArgTypes(parameterTypes, cache.argClass, false);
            if (!inputType.first) {
                continue;
            } else {
                cache.argTypes = inputType.second;
            }
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
    }
}


