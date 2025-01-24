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

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.io.Text;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TDictionaryTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Dictionary metadata, including its structure and data source information. saved in
 */
public class Dictionary extends Table {
    // TODO: dictionary should also be able to be created in external catalog.
    @SerializedName(value = "dbName")
    private final String dbName;

    // dict name use base class's name. these 3 is expected not to be null
    @SerializedName(value = "sourceCtlName")
    private final String sourceCtlName;
    @SerializedName(value = "sourceDbName")
    private final String sourceDbName;
    @SerializedName(value = "sourceTableName")
    private final String sourceTableName;

    @SerializedName(value = "columns")
    private final List<DictionaryColumnDefinition> columns;
    @SerializedName(value = "properties")
    private final Map<String, String> properties;

    // createTime saved in base class

    // lastUpdateTime in milliseconds
    @SerializedName(value = "lastUpdateTime")
    private long lastUpdateTime;

    public enum DictionaryStatus {
        LOADING, // wait load task finishs
        NORMAL, // normal status
        OUT_OF_DATE, // wait load task be scheduled
        REMOVING; // wait unload task be scheduled and finish
    }

    @SerializedName(value = "status")
    private DictionaryStatus status;
    @SerializedName(value = "layout")
    private final LayoutType layout;
    @SerializedName(value = "version")
    private long version; // every time dictionary is updated, version will increase by 1

    // we need this to call Table's constructor with no args which construct new rwLock and more.
    // for gson only and it will set variables soon. so no need to set them.
    public Dictionary() {
        super(TableType.DICTIONARY);
        this.dbName = null;
        this.sourceCtlName = null;
        this.sourceDbName = null;
        this.sourceTableName = null;
        this.columns = null;
        this.properties = null;
        this.lastUpdateTime = 0;
        this.status = null;
        this.layout = null;
        this.version = 0;
    }

    public Dictionary(CreateDictionaryInfo info, long uniqueId) {
        super(uniqueId, info.getDictName(), TableType.DICTIONARY, /* source table's schema as dict's FullSchema */info
                .getColumns().stream().map(DictionaryColumnDefinition::getOriginColumn).collect(Collectors.toList()));
        this.dbName = info.getDbName();
        this.sourceCtlName = info.getSourceCtlName();
        this.sourceDbName = info.getSourceDbName();
        this.sourceTableName = info.getSourceTableName();
        this.columns = info.getColumns();
        this.properties = info.getProperties();
        this.lastUpdateTime = createTime;
        this.status = DictionaryStatus.NORMAL;
        this.layout = info.getLayout();
        this.version = 1;
    }

    public String getDbName() {
        return dbName;
    }

    public String getSourceCtlName() {
        return sourceCtlName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public List<DictionaryColumnDefinition> getDicColumns() {
        return columns;
    }

    public Column getOriginColumn(String name) {
        for (DictionaryColumnDefinition column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column.getOriginColumn();
            }
        }
        throw new IllegalArgumentException("Column " + name + " not found in dictionary " + getName());
    }

    public List<String> getSourceQualifiedName() {
        List<String> qualifiedName = Lists.newArrayList();
        if (Strings.isNullOrEmpty(sourceCtlName) || Strings.isNullOrEmpty(sourceDbName)
                || Strings.isNullOrEmpty(sourceTableName)) {
            throw new IllegalArgumentException("dictionary's source name is not completed");
        }
        qualifiedName.add(sourceCtlName);
        qualifiedName.add(sourceDbName);
        qualifiedName.add(sourceTableName);
        return qualifiedName;
    }

    @Override
    public Database getDatabase() {
        return Env.getCurrentInternalCatalog().getDbNullable(dbName);
    }

    @Override
    public List<String> getFullQualifiers() {
        return ImmutableList.of(Env.getCurrentEnv().getInternalCatalog().getName(), dbName, getName());
    }

    public List<String> getColumnNames() {
        return columns.stream().map(DictionaryColumnDefinition::getName).collect(Collectors.toList());
    }

    public DataType getColumnType(String columnName) {
        for (DictionaryColumnDefinition column : columns) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return DataType.fromCatalogType(column.getType());
            }
        }
        throw new IllegalArgumentException("Column " + columnName + " not found in dictionary " + getName());
    }

    public DataType getKeyColumnType() {
        for (DictionaryColumnDefinition column : columns) {
            if (column.isKey()) {
                return DataType.fromCatalogType(column.getType());
            }
        }
        throw new IllegalArgumentException("Key column not found in dictionary " + getName());
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void increaseVersion() {
        this.version++;
    }

    public void decreaseVersion() {
        this.version--;
    }

    public long getVersion() {
        return version;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void updateLastUpdateTime() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public DictionaryStatus getStatus() {
        return status;
    }

    public void setStatus(DictionaryStatus status) {
        this.status = status;
    }

    public LayoutType getLayout() {
        return layout;
    }

    @Override
    public TTableDescriptor toThrift() {
        TDictionaryTable tDictionaryTable = new TDictionaryTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.DICTIONARY_TABLE, fullSchema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setDictionaryTable(tDictionaryTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Dictionary read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Dictionary.class);
    }

    @Override
    public String toString() {
        return "Dictionary{" + "dbName='" + dbName + '\'' + ", sourceCtlName='" + sourceCtlName + '\''
                + ", sourceDbName='" + sourceDbName + '\'' + ", sourceTableName='" + sourceTableName + '\''
                + ", columns=" + columns + ", properties=" + properties + ", version=" + version + ", lastUpdateTime="
                + lastUpdateTime + ", status=" + status + ", layout=" + layout + '}';
    }
}
