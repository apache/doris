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

package org.apache.doris.external.hive.util;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.FileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Hive util for create or query hive table.
 */
public final class HiveUtil {
    private static final Logger LOG = LogManager.getLogger(HiveUtil.class);

    private HiveUtil() {
    }

    /**
     * get input format class from inputFormatName.
     *
     * @param jobConf jobConf used when getInputFormatClass
     * @param inputFormatName inputFormat class name
     * @param symlinkTarget use target inputFormat class when inputFormat is SymlinkTextInputFormat
     * @return a class of inputFormat.
     * @throws UserException when class not found.
     */
    public static InputFormat<?, ?> getInputFormat(JobConf jobConf,
            String inputFormatName, boolean symlinkTarget) throws UserException {
        try {
            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        } catch (ClassNotFoundException | RuntimeException e) {
            throw new UserException("Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
            throws ClassNotFoundException {
        // CDH uses different names for Parquet
        if ("parquet.hive.DeprecatedParquetInputFormat".equals(inputFormatName)
                || "parquet.hive.MapredParquetInputFormat".equals(inputFormatName)) {
            return MapredParquetInputFormat.class;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }

    /**
     * transform hiveSchema to Doris schema.
     *
     * @param hiveSchema hive schema
     * @return doris schema
     * @throws AnalysisException when transform failed.
     */
    public static List<Column> transformHiveSchema(List<FieldSchema> hiveSchema) throws AnalysisException {
        List<Column> newSchema = Lists.newArrayList();
        for (FieldSchema hiveColumn : hiveSchema) {
            try {
                newSchema.add(HiveUtil.transformHiveField(hiveColumn));
            } catch (UnsupportedOperationException e) {
                LOG.warn("Unsupported data type in Doris, ignore column[{}], with error: {}",
                        hiveColumn.getName(), e.getMessage());
                throw e;
            }
        }
        return newSchema;
    }

    /**
     * tranform hive field to doris column.
     *
     * @param field hive field to be transformed
     * @return doris column
     */
    public static Column transformHiveField(FieldSchema field) {
        TypeInfo hiveTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
        Type type = convertHiveTypeToiveDoris(hiveTypeInfo);
        return new Column(field.getName(), type, false, null, true, null, field.getComment());
    }

    private static Type convertHiveTypeToiveDoris(TypeInfo hiveTypeInfo) {

        switch (hiveTypeInfo.getCategory()) {
            case PRIMITIVE: {
                PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) hiveTypeInfo;
                switch (primitiveTypeInfo.getPrimitiveCategory()) {
                    case VOID:
                        return Type.NULL;
                    case BOOLEAN:
                        return Type.BOOLEAN;
                    case BYTE:
                        return Type.TINYINT;
                    case SHORT:
                        return Type.SMALLINT;
                    case INT:
                        return Type.INT;
                    case LONG:
                        return Type.BIGINT;
                    case FLOAT:
                        return Type.FLOAT;
                    case DOUBLE:
                        return Type.DOUBLE;
                    case STRING:
                        return Type.STRING;
                    case CHAR:
                        return Type.CHAR;
                    case VARCHAR:
                        return Type.VARCHAR;
                    case DATE:
                        return ScalarType.getDefaultDateType(Type.DATE);
                    case TIMESTAMP:
                        return ScalarType.getDefaultDateType(Type.DATETIME);
                    case DECIMAL:
                        return Type.DECIMALV2;
                    default:
                        throw new UnsupportedOperationException("Unsupported type: "
                            + primitiveTypeInfo.getPrimitiveCategory());
                }
            }
            case LIST:
                TypeInfo elementTypeInfo = ((ListTypeInfo) hiveTypeInfo)
                        .getListElementTypeInfo();
                Type newType = null;
                if (elementTypeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    newType = convertHiveTypeToiveDoris(elementTypeInfo);
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + hiveTypeInfo.toString());
                }
                return new ArrayType(newType);
            case MAP:
            case STRUCT:
            case UNION:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + hiveTypeInfo.toString());
        }
    }

    public static boolean isSplittable(InputFormat<?, ?> inputFormat, Path path, JobConf jobConf) {
        // ORC uses a custom InputFormat but is always splittable
        if (inputFormat.getClass().getSimpleName().equals("OrcInputFormat")) {
            return true;
        }

        // use reflection to get isSplitable method on FileInputFormat
        // ATTN: the method name is actually "isSplitable", but the right spell is "isSplittable"
        Method method = null;
        for (Class<?> clazz = inputFormat.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            try {
                method = clazz.getDeclaredMethod("isSplitable", FileSystem.class, Path.class);
                break;
            } catch (NoSuchMethodException ignored) {
                LOG.debug("Class {} doesn't contain isSplitable method.", clazz);
            }
        }

        if (method == null) {
            return false;
        }
        try {
            method.setAccessible(true);
            return (boolean) method.invoke(inputFormat, FileSystemFactory.getNativeByPath(path, jobConf), path);
        } catch (InvocationTargetException | IllegalAccessException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getHivePartitionValue(String part) {
        String[] kv = part.split("=");
        Preconditions.checkState(kv.length == 2, String.format("Malformed partition name %s", part));
        try {
            // hive partition value maybe contains special characters like '=' and '/'
            return URLDecoder.decode(kv[1], StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // It should not be here
            throw new RuntimeException(e);
        }
    }

}
