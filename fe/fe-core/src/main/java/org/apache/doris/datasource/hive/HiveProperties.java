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

package org.apache.doris.datasource.hive;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HiveProperties {
    public static final String PROP_FIELD_DELIMITER = "field.delim";
    public static final String PROP_SERIALIZATION_FORMAT = "serialization.format";
    public static final String DEFAULT_FIELD_DELIMITER = "\1"; // "\x01"

    public static final String PROP_LINE_DELIMITER = "line.delim";
    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final String PROP_COLLECTION_DELIMITER_HIVE2 = "colelction.delim";
    public static final String PROP_COLLECTION_DELIMITER_HIVE3 = "collection.delim";
    public static final String DEFAULT_COLLECTION_DELIMITER = "\2";

    public static final String PROP_MAP_KV_DELIMITER = "mapkey.delim";
    public static final String DEFAULT_MAP_KV_DELIMITER = "\003";

    public static final String PROP_ESCAPE_DELIMITER = "escape.delim";
    public static final String DEFAULT_ESCAPE_DELIMIER = "\\";

    public static final String PROP_NULL_FORMAT = "serialization.null.format";
    public static final String DEFAULT_NULL_FORMAT = "\\N";

    // The following properties are used for OpenCsvSerde.
    public static final String PROP_SEPARATOR_CHAR = OpenCSVSerde.SEPARATORCHAR;
    public static final String DEFAULT_SEPARATOR_CHAR = ",";
    public static final String PROP_QUOTE_CHAR = OpenCSVSerde.QUOTECHAR;
    public static final String DEFAULT_QUOTE_CHAR = "\"";
    public static final String PROP_ESCAPE_CHAR = OpenCSVSerde.ESCAPECHAR;
    public static final String DEFAULT_ESCAPE_CHAR = "\\";

    public static final Set<String> HIVE_SERDE_PROPERTIES = ImmutableSet.of(
            PROP_FIELD_DELIMITER,
            PROP_COLLECTION_DELIMITER_HIVE2,
            PROP_COLLECTION_DELIMITER_HIVE3,
            PROP_SEPARATOR_CHAR,
            PROP_SERIALIZATION_FORMAT,
            PROP_LINE_DELIMITER,
            PROP_QUOTE_CHAR,
            PROP_MAP_KV_DELIMITER,
            PROP_ESCAPE_DELIMITER,
            PROP_ESCAPE_CHAR,
            PROP_NULL_FORMAT);

    public static String getFieldDelimiter(Table table) {
        // This method is used for text format.
        Optional<String> fieldDelim = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_FIELD_DELIMITER);
        Optional<String> serFormat = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_SERIALIZATION_FORMAT);
        return HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_FIELD_DELIMITER, fieldDelim, serFormat));
    }

    public static String getSeparatorChar(Table table) {
        Optional<String> separatorChar = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_SEPARATOR_CHAR);
        return HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_SEPARATOR_CHAR, separatorChar);
    }

    public static String getLineDelimiter(Table table) {
        Optional<String> lineDelim = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_LINE_DELIMITER);
        return HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_LINE_DELIMITER, lineDelim));
    }

    public static String getMapKvDelimiter(Table table) {
        Optional<String> mapkvDelim = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_MAP_KV_DELIMITER);
        return HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_MAP_KV_DELIMITER, mapkvDelim));
    }

    public static String getCollectionDelimiter(Table table) {
        Optional<String> collectionDelimHive2 = HiveMetaStoreClientHelper.getSerdeProperty(table,
                PROP_COLLECTION_DELIMITER_HIVE2);
        Optional<String> collectionDelimHive3 = HiveMetaStoreClientHelper.getSerdeProperty(table,
                PROP_COLLECTION_DELIMITER_HIVE3);
        return HiveMetaStoreClientHelper.getByte(HiveMetaStoreClientHelper.firstPresentOrDefault(
                DEFAULT_COLLECTION_DELIMITER, collectionDelimHive2, collectionDelimHive3));
    }

    public static Optional<String> getEscapeDelimiter(Table table) {
        Optional<String> escapeDelim = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_ESCAPE_DELIMITER);
        if (escapeDelim.isPresent()) {
            String escape = HiveMetaStoreClientHelper.getByte(escapeDelim.get());
            if (escape != null) {
                return Optional.of(escape);
            } else {
                return Optional.of(DEFAULT_ESCAPE_DELIMIER);
            }
        }
        return Optional.empty();
    }

    public static String getNullFormat(Table table) {
        Optional<String> nullFormat = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_NULL_FORMAT);
        return HiveMetaStoreClientHelper.firstPresentOrDefault(DEFAULT_NULL_FORMAT, nullFormat);
    }

    public static String getQuoteChar(Table table) {
        Optional<String> quoteChar = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_QUOTE_CHAR);
        return HiveMetaStoreClientHelper.firstPresentOrDefault(DEFAULT_QUOTE_CHAR, quoteChar);
    }

    public static String getEscapeChar(Table table) {
        Optional<String> escapeChar = HiveMetaStoreClientHelper.getSerdeProperty(table, PROP_ESCAPE_CHAR);
        return HiveMetaStoreClientHelper.firstPresentOrDefault(DEFAULT_ESCAPE_CHAR, escapeChar);
    }

    // Set properties to table
    public static void setTableProperties(Table table, Map<String, String> properties) {
        HashMap<String, String> serdeProps = new HashMap<>();
        HashMap<String, String> tblProps = new HashMap<>();

        for (String k : properties.keySet()) {
            if (HIVE_SERDE_PROPERTIES.contains(k)) {
                serdeProps.put(k, properties.get(k));
            } else {
                tblProps.put(k, properties.get(k));
            }
        }

        if (table.getParameters() == null) {
            table.setParameters(tblProps);
        } else {
            table.getParameters().putAll(tblProps);
        }

        if (table.getSd().getSerdeInfo().getParameters() == null) {
            table.getSd().getSerdeInfo().setParameters(serdeProps);
        } else {
            table.getSd().getSerdeInfo().getParameters().putAll(serdeProps);
        }
    }
}
