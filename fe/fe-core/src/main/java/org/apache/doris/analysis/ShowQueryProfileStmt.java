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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

// For stmt like:
// show query profile "/";   # list all saving query ids
// show query profile "/e0f7390f5363419e-b416a2a79996083e"  # show graph of fragments of the query
// show query profile "/e0f7390f5363419e-b416a2a79996083e/0" # show instance list of the specified fragment
// show query profile "/e0f7390f5363419e-b416a2a79996083e/0/e0f7390f5363419e-b416a2a799960906" # show graph of the instance
public class ShowQueryProfileStmt extends ShowStmt {
    // This should be same as ProfileManager.PROFILE_HEADERS
    public static final ShowResultSetMetaData META_DATA_QUERY_IDS =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("QueryId", ScalarType.createVarchar(128)))
                    .addColumn(new Column("User", ScalarType.createVarchar(128)))
                    .addColumn(new Column("DefaultDb", ScalarType.createVarchar(128)))
                    .addColumn(new Column("SQL", ScalarType.createVarchar(65535)))
                    .addColumn(new Column("QueryType", ScalarType.createVarchar(128)))
                    .addColumn(new Column("StartTime", ScalarType.createVarchar(128)))
                    .addColumn(new Column("EndTime", ScalarType.createVarchar(128)))
                    .addColumn(new Column("TotalTime", ScalarType.createVarchar(128)))
                    .addColumn(new Column("QueryState", ScalarType.createVarchar(128)))
                    .build();

    public static final ShowResultSetMetaData META_DATA_FRAGMENTS =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Fragments", ScalarType.createVarchar(65535)))
                    .build();
    public static final ShowResultSetMetaData META_DATA_INSTANCES =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Instances", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Host", ScalarType.createVarchar(64)))
                    .addColumn(new Column("ActiveTime", ScalarType.createVarchar(64)))
                    .build();
    public static final ShowResultSetMetaData META_DATA_SINGLE_INSTANCE =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Instance", ScalarType.createVarchar(65535)))
                    .build();

    public enum PathType {
        QUERY_IDS,
        FRAGMETNS,
        INSTANCES,
        SINGLE_INSTANCE
    }

    private String queryIdPath;
    private PathType pathType;

    private String queryId = "";
    private String fragmentId = "";
    private String instanceId = "";

    public ShowQueryProfileStmt(String queryIdPath) {
        this.queryIdPath = queryIdPath;
    }

    public PathType getPathType() {
        return pathType;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getFragmentId() {
        return fragmentId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(queryIdPath)) {
            // list all query ids
            pathType = PathType.QUERY_IDS;
            return;
        }

        if (!queryIdPath.startsWith("/")) {
            throw new AnalysisException("Query path must starts with '/'");
        }
        pathType = PathType.QUERY_IDS;
        String[] parts = queryIdPath.split("/");
        if (parts.length > 4) {
            throw new AnalysisException("Query path must in format '/queryId/fragmentId/instanceId'");
        }

        for (int i = 0; i < parts.length; i++) {
            switch (i) {
                case 0:
                    pathType = PathType.QUERY_IDS;
                    continue;
                case 1:
                    queryId = parts[i];
                    pathType = PathType.FRAGMETNS;
                    break;
                case 2:
                    fragmentId = parts[i];
                    pathType = PathType.INSTANCES;
                    break;
                case 3:
                    instanceId = parts[i];
                    pathType = PathType.SINGLE_INSTANCE;
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW QUERY PROFILE ").append(queryIdPath);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        switch (pathType) {
            case QUERY_IDS:
                return META_DATA_QUERY_IDS;
            case FRAGMETNS:
                return META_DATA_FRAGMENTS;
            case INSTANCES:
                return META_DATA_INSTANCES;
            case SINGLE_INSTANCE:
                return META_DATA_SINGLE_INSTANCE;
            default:
                return null;
        }
    }
}
