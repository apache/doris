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

import com.google.common.collect.Lists;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;

import org.apache.hadoop.conf.Configuration;
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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.reflect.Field;
import java.util.List;

public final class HiveUtil {
    private static final Logger LOG = LogManager.getLogger(HiveUtil.class);

    private static final DateTimeFormatter HIVE_DATE_PARSER = ISODateTimeFormat.date().withZoneUTC();
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER;
    private static final Field COMPRESSION_CODECS_FIELD;

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSSSS").getParser(),
        };
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getPrinter();
        HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder().append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser).toFormatter().withZoneUTC();

        try {
            COMPRESSION_CODECS_FIELD = TextInputFormat.class.getDeclaredField("compressionCodecs");
            COMPRESSION_CODECS_FIELD.setAccessible(true);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private HiveUtil()
    {
    }

    public static InputFormat<?, ?> getInputFormat(Configuration configuration, String inputFormatName, boolean symlinkTarget) throws UserException {
        try {
            JobConf jobConf = new JobConf(configuration);

            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (ClassNotFoundException | RuntimeException e) {
            throw new UserException("Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
            throws ClassNotFoundException
    {
        // CDH uses different names for Parquet
        if ("parquet.hive.DeprecatedParquetInputFormat".equals(inputFormatName) ||
                "parquet.hive.MapredParquetInputFormat".equals(inputFormatName)) {
            return MapredParquetInputFormat.class;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }

    public static List<Column> transformHiveSchema(List<FieldSchema> hiveSchema) throws AnalysisException {
        List<Column> newSchema = Lists.newArrayList();
        for (FieldSchema hiveColumn : hiveSchema) {
            try {
                newSchema.add(HiveUtil.transformHiveField(hiveColumn));
            } catch (UnsupportedOperationException e) {
                if (Config.iceberg_table_creation_strict_mode) {
                    throw e;
                }
                LOG.warn("Unsupported data type in Doris, ignore column[{}], with error: {}",
                        hiveColumn.getName(), e.getMessage());
                continue;
            }
        }
        return newSchema;
    }

    public static Column transformHiveField(FieldSchema field) {
        TypeInfo hiveTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
        Type type = convertHiveTypeToiveDoris(hiveTypeInfo);
        return new Column(field.getName(), type, false, null, true, null, field.getComment());
    }

    private static Type convertHiveTypeToiveDoris(TypeInfo hiveTypeInfo) {

        switch (hiveTypeInfo.getCategory()) {
            case PRIMITIVE: {
                PrimitiveTypeInfo pTypeInfo = (PrimitiveTypeInfo) hiveTypeInfo;
                switch (pTypeInfo.getPrimitiveCategory()) {
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
                        return Type.DATE;
                    case TIMESTAMP:
                        return Type.DATETIME;
                    case DECIMAL:
                        return Type.DECIMALV2;
                    default:
                        throw new UnsupportedOperationException("Unsupported type: " + pTypeInfo.getPrimitiveCategory());
                }
            }
            case LIST:
                TypeInfo elementTypeInfo = ((ListTypeInfo)hiveTypeInfo)
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

}
