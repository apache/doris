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

package org.apache.doris.tablefunction;

import com.google.common.base.Preconditions;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcMySQLClient;
import org.apache.doris.load.cdcload.DataSourceType;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TFileType;

import java.util.List;
import java.util.Map;

import static org.apache.doris.load.cdcload.CdcLoadConstants.DATABASE_NAME;
import static org.apache.doris.load.cdcload.CdcLoadConstants.DB_SOURCE_TYPE;
import static org.apache.doris.load.cdcload.CdcLoadConstants.HOST_NAME;
import static org.apache.doris.load.cdcload.CdcLoadConstants.PASSWORD;
import static org.apache.doris.load.cdcload.CdcLoadConstants.PORT;
import static org.apache.doris.load.cdcload.CdcLoadConstants.TABLE_NAME;
import static org.apache.doris.load.cdcload.CdcLoadConstants.USERNAME;

/**
 * The Implement of table valued function
 * cdc("db_source_type" = "mysql")
 */
public class CdcTableValuedFunction extends ExternalFileTableValuedFunction {

    public static final String NAME = "cdc";
    private Map<String, String> properties;

    public CdcTableValuedFunction(Map<String, String> properties) {
        checkRequiredProperties();
        this.properties = properties;
    }

    private void checkRequiredProperties() {
        Preconditions.checkNotNull(properties.get(DB_SOURCE_TYPE), "db_source_type is required");
        Preconditions.checkNotNull(properties.get(HOST_NAME), "hostname is required");
        Preconditions.checkNotNull(properties.get(PORT), "port is required");
        Preconditions.checkNotNull(properties.get(USERNAME), "username is required");
        Preconditions.checkNotNull(properties.get(PASSWORD), "password is required");
        Preconditions.checkNotNull(properties.get(DATABASE_NAME), "database_name is required");
        Preconditions.checkNotNull(properties.get(TABLE_NAME), "table_name is required");
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        DataSourceType sourceType = DataSourceType.getByType(properties.get(DB_SOURCE_TYPE));
        JdbcClientConfig config = new JdbcClientConfig();
        config.setCatalog(properties.get(DB_SOURCE_TYPE));
        config.setUser(properties.get(USERNAME));
        config.setPassword(properties.get(PASSWORD));
        config.setDriverUrl(sourceType.getDriverUrl());
        config.setJdbcUrl("jdbc:" + properties.get(DB_SOURCE_TYPE) + "://" + properties.get(HOST_NAME) + ":" + properties.get(PORT) + "/");
        switch (sourceType) {
            case MYSQL:
                JdbcClient client = JdbcMySQLClient.createJdbcClient(config);
                List<Column> columns = client.getColumnsFromJdbc(properties.get(DATABASE_NAME), properties.get(TABLE_NAME));
                return columns;
            default:
                throw new AnalysisException("Unsupported source type " + properties.get(DB_SOURCE_TYPE));
        }
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc) {
        //set cdc scannode
        return null;
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public String getFilePath() {
        return null;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("CdcTvfBroker", StorageBackend.StorageType.STREAM, locationProperties);
    }

    @Override
    public String getTableName() {
        return "CdcTableValuedFunction";
    }
}
