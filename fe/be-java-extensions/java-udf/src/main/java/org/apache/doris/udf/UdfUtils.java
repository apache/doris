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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TPrimitiveType;

import java.util.ArrayList;
import java.util.Set;

/**
 * Matches a user function's Java signature against the column types the function was declared
 * with.
 *
 * <p>Both methods answer the same question in two halves - does this Java class stand for that
 * Doris type, and if so with which precision and scale - and both return a false first element
 * rather than throwing when the answer is no, because the caller tries every overload of
 * {@code evaluate} in turn and only reports a failure once none of them matched.
 */
public class UdfUtils {

    private UdfUtils() {
    }

    /**
     * Sets the return type of a Java UDF. Returns true if the return type is compatible
     * with the return type from the function definition. Throws an InternalException
     * if the return type is not supported.
     */
    public static Pair<Boolean, JavaUdfDataType> setReturnType(Type retType, Class<?> udfReturnType)
            throws InternalException {
        if (!JavaUdfDataType.isSupported(retType)) {
            throw new InternalException("Unsupported return type: " + retType.toSql());
        }
        Set<JavaUdfDataType> javaTypes = JavaUdfDataType.getCandidateTypes(udfReturnType);
        // Check if the evaluate method return type is compatible with the return type from
        // the function definition. This happens when both of them map to the same primitive
        // type.
        Object[] res = javaTypes.stream().filter(t -> {
            TPrimitiveType t1 = t.getPrimitiveType();
            TPrimitiveType ret = retType.getPrimitiveType().toThrift();
            return (t1 == ret) || (t1 == TPrimitiveType.STRING && ret == TPrimitiveType.VARCHAR);
        }).toArray();

        JavaUdfDataType result = new JavaUdfDataType(
                res.length == 0 ? javaTypes.iterator().next() : (JavaUdfDataType) res[0]);
        if (retType.isDecimalV3() || retType.isDatetimeV2() || retType.isTimeStampTz()) {
            result.setPrecision(retType.getPrecision());
            result.setScale(((ScalarType) retType).getScalarScale());
        } else if (retType.isArrayType()) {
            ArrayType arrType = (ArrayType) retType;
            result = new JavaUdfArrayType(arrType.getItemType());
            if (arrType.getItemType().isDatetimeV2() || arrType.isTimeStampTz()
                    || arrType.getItemType().isDecimalV3()) {
                result.setPrecision(arrType.getItemType().getPrecision());
                result.setScale(((ScalarType) arrType.getItemType()).getScalarScale());
            }
        } else if (retType.isMapType()) {
            MapType mapType = (MapType) retType;
            Type keyType = mapType.getKeyType();
            Type valuType = mapType.getValueType();
            result = new JavaUdfMapType(keyType, valuType);
            JavaUdfMapType udfMapType = ((JavaUdfMapType) result);
            if (keyType.isDatetimeV2() || keyType.isTimeStampTz() || keyType.isDecimalV3()) {
                udfMapType.setKeyScale(((ScalarType) keyType).getScalarScale());
            }
            if (valuType.isDatetimeV2() || valuType.isTimeStampTz() || valuType.isDecimalV3()) {
                udfMapType.setValueScale(((ScalarType) valuType).getScalarScale());
            }
        } else if (retType.isStructType()) {
            StructType structType = (StructType) retType;
            result = new JavaUdfStructType(structType.getFields());
        }
        return Pair.of(res.length != 0, result);
    }

    /**
     * Sets the argument types of a Java UDF or UDAF. Returns true if the argument types specified
     * in the UDF are compatible with the argument types of the evaluate() function loaded
     * from the associated JAR file.
     *
     * @throws InternalException
     */
    public static Pair<Boolean, JavaUdfDataType[]> setArgTypes(Type[] parameterTypes, Class<?>[] udfArgTypes,
            boolean isUdaf) throws InternalException {
        JavaUdfDataType[] inputArgTypes = new JavaUdfDataType[parameterTypes.length];
        int firstPos = isUdaf ? 1 : 0;
        for (int i = 0; i < parameterTypes.length; ++i) {
            Set<JavaUdfDataType> javaTypes = JavaUdfDataType.getCandidateTypes(udfArgTypes[i + firstPos]);
            int finalI = i;
            Object[] res = javaTypes.stream().filter(t -> {
                TPrimitiveType t1 = t.getPrimitiveType();
                TPrimitiveType param = parameterTypes[finalI].getPrimitiveType().toThrift();
                return (t1 == param) || (t1 == TPrimitiveType.STRING && param == TPrimitiveType.VARCHAR);
            }).toArray();
            inputArgTypes[i] = new JavaUdfDataType(
                    res.length == 0 ? javaTypes.iterator().next() : (JavaUdfDataType) res[0]);
            if (parameterTypes[finalI].isDecimalV3() || parameterTypes[finalI].isDatetimeV2()
                    || parameterTypes[finalI].isTimeStampTz()) {
                inputArgTypes[i].setPrecision(parameterTypes[finalI].getPrecision());
                inputArgTypes[i].setScale(((ScalarType) parameterTypes[finalI]).getScalarScale());
            } else if (parameterTypes[finalI].isArrayType()) {
                ArrayType arrType = (ArrayType) parameterTypes[finalI];
                inputArgTypes[i] = new JavaUdfArrayType(arrType.getItemType());
                if (arrType.getItemType().isDatetimeV2() || arrType.isTimeStampTz()
                        || arrType.getItemType().isDecimalV3()) {
                    inputArgTypes[i].setPrecision(arrType.getItemType().getPrecision());
                    inputArgTypes[i].setScale(((ScalarType) arrType.getItemType()).getScalarScale());
                }
            } else if (parameterTypes[finalI].isMapType()) {
                MapType mapType = (MapType) parameterTypes[finalI];
                Type keyType = mapType.getKeyType();
                Type valuType = mapType.getValueType();
                inputArgTypes[i] = new JavaUdfMapType(keyType, valuType);
                JavaUdfMapType udfMapType = ((JavaUdfMapType) inputArgTypes[i]);
                if (keyType.isDatetimeV2() || keyType.isTimeStampTz() || keyType.isDecimalV3()) {
                    udfMapType.setKeyScale(((ScalarType) keyType).getScalarScale());
                }
                if (valuType.isDatetimeV2() || keyType.isTimeStampTz() || valuType.isDecimalV3()) {
                    udfMapType.setValueScale(((ScalarType) valuType).getScalarScale());
                }
            } else if (parameterTypes[finalI].isStructType()) {
                StructType structType = (StructType) parameterTypes[finalI];
                ArrayList<StructField> fields = structType.getFields();
                inputArgTypes[i] = new JavaUdfStructType(fields);
            } else if (parameterTypes[finalI].isIP()) {
                if (parameterTypes[finalI].isIPv4()) {
                    inputArgTypes[i] = new JavaUdfDataType(JavaUdfDataType.IPV4);
                } else {
                    inputArgTypes[i] = new JavaUdfDataType(JavaUdfDataType.IPV6);
                }
            }
            if (res.length == 0) {
                return Pair.of(false, inputArgTypes);
            }
        }
        return Pair.of(true, inputArgTypes);
    }
}
