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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command for SHOW STAGES.
 */
public class ShowStagesCommand extends ShowCommand {

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

    public ShowStagesCommand() {
        super(PlanType.SHOW_STAGES_COMMAND);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        try {
            List<Cloud.StagePB> stages = ((CloudInternalCatalog) Env.getCurrentInternalCatalog())
                                            .getStage(Cloud.StagePB.StageType.EXTERNAL, null, null, null);
            if (stages == null) {
                throw new AnalysisException("get stage err");
            }
            List<List<String>> results = new ArrayList<>();
            for (Cloud.StagePB stage : stages) {
                // todo(copy into): check priv
                // if (!Env.getCurrentEnv().getAuth()
                //         .checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(), stage.getName(),
                //                 PrivPredicate.USAGE, ResourceTypeEnum.STAGE)) {
                //     continue;
                // }
                List<String> result = new ArrayList<>();
                result.add(stage.getName());
                result.add(stage.getStageId());
                result.add(stage.getObjInfo().getEndpoint());
                result.add(stage.getObjInfo().getRegion());
                result.add(stage.getObjInfo().getBucket());
                result.add(stage.getObjInfo().getPrefix());
                result.add(StringUtils.isEmpty(stage.getObjInfo().getAk()) ? "" : "**********");
                result.add(StringUtils.isEmpty(stage.getObjInfo().getSk()) ? "" : "**********");
                result.add(stage.getObjInfo().getProvider().name());
                Map<String, String> propertiesMap = new HashMap<>();
                propertiesMap.putAll(stage.getPropertiesMap());
                result.add(new GsonBuilder().disableHtmlEscaping().create().toJson(propertiesMap));
                result.add(stage.getComment());
                result.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(stage.getCreateTime())));
                result.add(stage.hasAccessType() ? stage.getAccessType().name()
                        : (StringUtils.isEmpty(stage.getObjInfo().getSk()) ? "" : "AKSK"));
                result.add(stage.getRoleName());
                result.add(stage.getArn());
                results.add(result);
            }
            return new ShowResultSet(getMetaData(), results);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowStagesCommand(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
