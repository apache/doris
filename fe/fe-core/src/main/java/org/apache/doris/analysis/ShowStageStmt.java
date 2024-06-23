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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// SHOW STAGES
//
// syntax:
//      SHOW STAGES
public class ShowStageStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowStageStmt.class);

    private static final String NAME_COL = "StageName";
    private static final String ID_COL = "StageId";
    private static final String ENDPOINT_COL = "Endpoint";
    private static final String REGION_COL = "Region";
    private static final String BUCKET_COL = "Bucket";
    private static final String PREFIX_COL = "Prefix";
    private static final String AK_COL = "AK";
    private static final String SK_COL = "SK";
    private static final String PROVIDER_COL = "Provider";
    private static final String DEFAULT_PROP_COL = "DefaultProperties";
    private static final String COMMENT = "Comment";
    private static final String CREATE_TIME = "CreateTime";
    private static final String ACCESS_TYPE = "AccessType";
    private static final String ROLE_NAME = "RoleName";
    private static final String ARN = "Arn";

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(64)))
                    .addColumn(new Column(ID_COL, ScalarType.createVarchar(64)))
                    .addColumn(new Column(ENDPOINT_COL, ScalarType.createVarchar(128)))
                    .addColumn(new Column(REGION_COL, ScalarType.createVarchar(64)))
                    .addColumn(new Column(BUCKET_COL, ScalarType.createVarchar(64)))
                    .addColumn(new Column(PREFIX_COL, ScalarType.createVarchar(128)))
                    .addColumn(new Column(AK_COL, ScalarType.createVarchar(128)))
                    .addColumn(new Column(SK_COL, ScalarType.createVarchar(128)))
                    .addColumn(new Column(PROVIDER_COL, ScalarType.createVarchar(10)))
                    .addColumn(new Column(DEFAULT_PROP_COL, ScalarType.createVarchar(512)))
                    .addColumn(new Column(COMMENT, ScalarType.createVarchar(512)))
                    .addColumn(new Column(CREATE_TIME, ScalarType.createVarchar(64)))
                    .addColumn(new Column(ACCESS_TYPE, ScalarType.createVarchar(64)))
                    .addColumn(new Column(ROLE_NAME, ScalarType.createVarchar(64)))
                    .addColumn(new Column(ARN, ScalarType.createVarchar(64)))
                    .build();

    public ShowStageStmt() {
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
    }

    @Override
    public String toSql() {
        return "SHOW STAGES";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
