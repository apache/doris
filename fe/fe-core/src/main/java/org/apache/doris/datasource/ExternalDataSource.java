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

package org.apache.doris.datasource;

import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang.NotImplementedException;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The abstract class for all types of external data sources.
 */
@Data
public abstract class ExternalDataSource implements DataSourceIf<ExternalDatabase>, Writable {
    // Unique id of this data source, will be assigned after data source is loaded.
    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected String type;
    // save properties of this data source, such as hive meta store url.
    @SerializedName(value = "dsProperty")
    protected DataSourceProperty dsProperty = new DataSourceProperty();

    /**
     * @return names of database in this data source.
     */
    public abstract List<String> listDatabaseNames(SessionContext ctx);

    /**
     * @param dbName
     * @return names of tables in specified database
     */
    public abstract List<String> listTableNames(SessionContext ctx, String dbName);

    /**
     * check if the specified table exist.
     *
     * @param dbName
     * @param tblName
     * @return true if table exists, false otherwise
     */
    public abstract boolean tableExist(SessionContext ctx, String dbName, String tblName);

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public List<String> getDbNames() {
        return listDatabaseNames(null);
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(String dbName) {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public ExternalDatabase getDbNullable(long dbId) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, String> getProperties() {
        return dsProperty.getProperties();
    }

    @Override
    public void modifyDatasourceName(String name) {
        this.name = name;
    }

    @Override
    public void modifyDatasourceProps(Map<String, String> props) {
        dsProperty.setProperties(props);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * External catalog has no cluster semantics.
     */
    protected static String getRealTableName(String tableName) {
        return ClusterNamespace.getNameFromFullName(tableName);
    }

    public static ExternalDataSource read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalDataSource.class);
    }
}
