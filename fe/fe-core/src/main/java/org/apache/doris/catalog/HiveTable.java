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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * External hive table
 * Currently only support loading from hive table
 */
public class HiveTable extends Table {
    private static final Logger LOG = LogManager.getLogger(HiveTable.class);

    private static final String PROPERTY_MISSING_MSG = "Hive %s is null. Please add properties('%s'='xxx') when create table";

    private static final String HIVE_DB = "database";
    private static final String HIVE_TABLE = "table";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String HIVE_HDFS_PREFIX = "dfs";

    private String hiveDb;
    private String hiveTable;
    private Map<String, String> hiveProperties = Maps.newHashMap();

    private boolean isLocalSchema = true;
    private Pattern digitPattern = null;

    public HiveTable() {
        super(TableType.HIVE);
    }

    public HiveTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.HIVE, schema);
        validate(properties);
        if (schema == null || schema.size() == 0) {
            isLocalSchema = false;
            digitPattern = Pattern.compile("(\\d+)");
        }
    }

    public String getHiveDbTable() {
        return String.format("%s.%s", hiveDb, hiveTable);
    }

    public String getHiveDb() {
        return hiveDb;
    }

    public String getHiveTable() {
        return hiveTable;
    }

    public Map<String, String> getHiveProperties() {
        return hiveProperties;
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        if (isLocalSchema) {
            return super.getBaseSchema(full);
        }
        List<Column> hiveMetastoreSchema = Lists.newArrayList();
        try {
            for (FieldSchema field : HiveMetaStoreClientHelper.getSchema(this)) {
                hiveMetastoreSchema.add(new Column(field.getName(), convertToDorisType(field.getType()),
                        true, null, true, null, field.getComment()));
            }
        } catch (DdlException e) {
            LOG.warn("Failed to get schema of hive table. DB {}, Table {}. {}",
                    this.hiveDb, this.hiveTable, e.getMessage());
            return null;
        }
        fullSchema = hiveMetastoreSchema;
        return fullSchema;
    }

    @Override
    public Column getColumn(String name) {
        if (isLocalSchema) {
            return nameToColumn.get(name);
        }
        Column col = null;
        if (fullSchema == null || fullSchema.size() == 0) {
            getBaseSchema(true);
        }
        for (Column column : fullSchema) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return col;
    }

    private Type convertToDorisType(String hiveType) {
        String lowerCaseType = hiveType.toLowerCase();
        if (lowerCaseType.equals("boolean")) {
            return Type.BOOLEAN;
        }
        if (lowerCaseType.equals("tinyint")) {
            return Type.TINYINT;
        }
        if (lowerCaseType.equals("smallint")) {
            return Type.SMALLINT;
        }
        if (lowerCaseType.equals("int")) {
            return Type.INT;
        }
        if (lowerCaseType.equals("bigint")) {
            return Type.BIGINT;
        }
        if (lowerCaseType.startsWith("char")) {
            ScalarType type = ScalarType.createType(PrimitiveType.CHAR);
            Matcher match = digitPattern.matcher(lowerCaseType);
            if (match.find()) {
                type.setLength(Integer.parseInt(match.group(1)));
            }
            return type;
        }
        if (lowerCaseType.startsWith("varchar")) {
            ScalarType type = ScalarType.createType(PrimitiveType.VARCHAR);
            Matcher match = digitPattern.matcher(lowerCaseType);
            if (match.find()) {
                type.setLength(Integer.parseInt(match.group(1)));
            }
            return type;
        }
        if (lowerCaseType.startsWith("decimal")) {
            Matcher match = digitPattern.matcher(lowerCaseType);
            int precision = ScalarType.DEFAULT_PRECISION;
            int scale = ScalarType.DEFAULT_SCALE;
            if (match.find()) {
                precision = Integer.parseInt(match.group(1));
            }
            if (match.find()) {
                scale = Integer.parseInt(match.group(1));
            }
            return ScalarType.createDecimalV2Type(precision, scale);
        }
        if (lowerCaseType.equals("date")) {
            return Type.DATE;
        }
        if (lowerCaseType.equals("timestamp")) {
            return Type.DATETIME;
        }
        LOG.warn("Hive type {} may not supported, will use STRING instead.", hiveType);
        return Type.STRING;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of hive table, "
                    + "they are: database, table and 'hive.metastore.uris'");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        hiveDb = copiedProps.get(HIVE_DB);
        if (Strings.isNullOrEmpty(hiveDb)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_DB, HIVE_DB));
        }
        copiedProps.remove(HIVE_DB);

        hiveTable = copiedProps.get(HIVE_TABLE);
        if (Strings.isNullOrEmpty(hiveTable)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_TABLE, HIVE_TABLE));
        }
        copiedProps.remove(HIVE_TABLE);

        // check hive properties
        // hive.metastore.uris
        String hiveMetastoreUris = copiedProps.get(HIVE_METASTORE_URIS);
        if (Strings.isNullOrEmpty(hiveMetastoreUris)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_METASTORE_URIS, HIVE_METASTORE_URIS));
        }
        copiedProps.remove(HIVE_METASTORE_URIS);
        hiveProperties.put(HIVE_METASTORE_URIS, hiveMetastoreUris);

        if (!copiedProps.isEmpty()) {
            Iterator<Map.Entry<String, String>> iter = copiedProps.entrySet().iterator();
            while(iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                if (entry.getKey().startsWith(HIVE_HDFS_PREFIX)) {
                    hiveProperties.put(entry.getKey(), entry.getValue());
                    iter.remove();
                }
            }
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, hiveDb);
        Text.writeString(out, hiveTable);
        out.writeInt(hiveProperties.size());
        for (Map.Entry<String, String> entry : hiveProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
        out.writeBoolean(isLocalSchema);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        hiveDb = Text.readString(in);
        hiveTable = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String val = Text.readString(in);
            hiveProperties.put(key, val);
        }
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_110) {
            isLocalSchema = in.readBoolean();
            if (!isLocalSchema) {
                digitPattern = Pattern.compile("(\\d+)");
            }
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        THiveTable tHiveTable = new THiveTable(getHiveDb(), getHiveTable(), getHiveProperties());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
    }
}
