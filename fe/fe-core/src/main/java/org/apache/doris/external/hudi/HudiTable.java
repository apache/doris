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

package org.apache.doris.external.hudi;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.THudiTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * External Hudi table.
 */
public class HudiTable extends Table {
    private static final Logger LOG = LogManager.getLogger(HudiTable.class);

    // table properties of this hudi table
    private Map<String, String> tableProperties = Maps.newHashMap();
    // remote Hudi database name in hive metastore
    private String hmsDatabaseName;
    // remote Hudi table name in hive metastore
    private String hmsTableName;

    public HudiTable() {
        super(TableType.HUDI);
    }

    /**
     * Generate a Hudi Table with id, name, schema, properties.
     *
     * @param id table id
     * @param tableName table name
     * @param fullSchema table's schema
     * @param tableProperties table's properties
     */
    public HudiTable(long id, String tableName, List<Column> fullSchema, Map<String, String>  tableProperties) {
        super(id, tableName, TableType.HUDI, fullSchema);
        this.tableProperties = tableProperties;
        this.hmsDatabaseName = tableProperties.get(HudiProperty.HUDI_DATABASE);
        this.hmsTableName = tableProperties.get(HudiProperty.HUDI_TABLE);
    }

    public String getHmsDatabaseName() {
        return hmsDatabaseName;
    }

    public String getHmsTableName() {
        return hmsTableName;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    public String getHmsTableIdentifer() {
        return String.format("%s.%s", hmsDatabaseName, hmsTableName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, hmsDatabaseName);
        Text.writeString(out, hmsTableName);

        out.writeInt(tableProperties.size());
        for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        hmsDatabaseName = Text.readString(in);
        hmsTableName = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            tableProperties.put(key, value);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        THudiTable thriftHudiTable = new THudiTable();
        thriftHudiTable.setDbName(getHmsDatabaseName());
        thriftHudiTable.setTableName(getHmsTableName());
        thriftHudiTable.setProperties(getTableProperties());

        TTableDescriptor thriftTableDescriptor = new TTableDescriptor(getId(), TTableType.BROKER_TABLE,
                fullSchema.size(), 0, getName(), "");
        thriftTableDescriptor.setHudiTable(thriftHudiTable);
        return thriftTableDescriptor;
    }
}
