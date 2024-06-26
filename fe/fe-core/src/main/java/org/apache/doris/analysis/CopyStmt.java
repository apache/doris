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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.stage.StageUtil;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.property.constants.BosProperties;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.load.loadv2.LoadTask.MergeType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copy statement
 */
public class CopyStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CopyStmt.class);

    private static final ShowResultSetMetaData COPY_INTO_META_DATA =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("id", ScalarType.createVarchar(64)))
                .addColumn(new Column("state", ScalarType.createVarchar(64)))
                .addColumn(new Column("type", ScalarType.createVarchar(64)))
                .addColumn(new Column("msg", ScalarType.createVarchar(128)))
                .addColumn(new Column("loadedRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("filterRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("unselectRows", ScalarType.createVarchar(64)))
                .addColumn(new Column("url", ScalarType.createVarchar(128)))
            .build();
    public static final String S3_BUCKET = "bucket";
    public static final String S3_PREFIX = "prefix";
    private static final String SET_VAR_KEY = "set_var";

    @Getter
    private final TableName tableName;
    @Getter
    private CopyFromParam copyFromParam;
    @Getter
    private CopyIntoProperties copyIntoProperties;
    private Map<String, String> optHints;

    private LabelName label = null;
    private BrokerDesc brokerDesc = null;
    private DataDescription dataDescription = null;
    private final Map<String, String> brokerProperties = new HashMap<>();
    private final Map<String, String> properties = new HashMap<>();

    @Getter
    private String stage;
    @Getter
    private String stageId;
    @Getter
    private StageType stageType;
    @Getter
    private String stagePrefix;
    @Getter
    private ObjectInfo objectInfo;
    private String userName;

    /**
     * Use for cup.
     */
    public CopyStmt(TableName tableName, List<String> cols, CopyFromParam copyFromParam,
            Map<String, String> properties, Map<String, Map<String, String>> optHints) {
        this.tableName = tableName;
        this.copyFromParam = copyFromParam;
        this.copyFromParam.setTargetColumns(cols);
        this.stage = copyFromParam.getStageAndPattern().getStageName();
        this.copyIntoProperties = new CopyIntoProperties(properties);
        if (optHints != null) {
            this.optHints = optHints.get(SET_VAR_KEY);
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (this.optHints != null && this.optHints.containsKey(SessionVariable.CLOUD_CLUSTER)) {
            ((CloudEnv) Env.getCurrentEnv()).checkCloudClusterPriv(this.optHints.get(SessionVariable.CLOUD_CLUSTER));
        }
        // generate a label
        String labelName = "copy_" + DebugUtil.printId(analyzer.getContext().queryId()).replace("-", "_");
        label = new LabelName(tableName.getDb(), labelName);
        label.analyze(analyzer);
        // analyze stage
        analyzeStageName();
        this.userName = ClusterNamespace.getNameFromFullName(
            ConnectContext.get().getCurrentUserIdentity().getQualifiedUser());
        analyze(userName, label.getDbName(), true);
    }

    private void analyze(String user, String db, boolean checkAuth) throws AnalysisException, DdlException {
        // get stage from meta service
        StagePB stagePB = StageUtil.getStage(stage, user, checkAuth);
        analyzeStagePB(stagePB);

        // generate broker desc
        brokerDesc = new BrokerDesc("S3", StorageBackend.StorageType.S3, brokerProperties);
        // generate data description
        String filePath = "s3://" + brokerProperties.get(S3_BUCKET) + "/" + brokerProperties.get(S3_PREFIX);
        Separator separator = copyIntoProperties.getColumnSeparator() != null ? new Separator(
                copyIntoProperties.getColumnSeparator()) : null;
        String fileFormatStr = copyIntoProperties.getFileType();
        Map<String, String> dataDescProperties = copyIntoProperties.getDataDescriptionProperties();
        copyFromParam.analyze(db, tableName, this.copyIntoProperties.useDeleteSign(),
                copyIntoProperties.getFileTypeIgnoreCompression());
        if (LOG.isDebugEnabled()) {
            LOG.debug("copy into params. sql: {}, fileColumns: {}, columnMappingList: {}, filter: {}",
                    getOrigStmt() != null ? getOrigStmt().originStmt : "", copyFromParam.getFileColumns(),
                    copyFromParam.getColumnMappingList(), copyFromParam.getFileFilterExpr());
        }
        dataDescription = new DataDescription(tableName.getTbl(), null, Lists.newArrayList(filePath),
                copyFromParam.getFileColumns(), separator, fileFormatStr, null, false,
                copyFromParam.getColumnMappingList(), copyFromParam.getFileFilterExpr(), null, MergeType.APPEND, null,
                null, dataDescProperties);
        dataDescription.setCompressType(StageUtil.parseCompressType(copyIntoProperties.getCompression()));
        if (!(copyFromParam.getColumnMappingList() == null
                || copyFromParam.getColumnMappingList().isEmpty())) {
            dataDescription.setIgnoreCsvRedundantCol(true);
        }
        // analyze data description
        if (checkAuth) {
            dataDescription.analyze(db);
        } else {
            dataDescription.analyzeWithoutCheckPriv(db);
        }
        String path;
        for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
            path = dataDescription.getFilePaths().get(i);
            dataDescription.getFilePaths().set(i, BosProperties.convertPathToS3(path));
            StorageBackend.checkPath(path, brokerDesc.getStorageType(), null);
            dataDescription.getFilePaths().set(i, path);
        }

        try {
            properties.putAll(copyIntoProperties.getExecProperties());
            // TODO support exec params as LoadStmt
            LoadStmt.checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void analyzeWhenReplay(String user, String db) throws UserException {
        analyze(user, db, false);
    }

    private void analyzeStageName() throws AnalysisException {
        if (stage.isEmpty()) {
            throw new AnalysisException("Stage name can not be empty");
        }
    }

    // after analyzeStagePB, fileFormat and copyOption is not null
    private void analyzeStagePB(StagePB stagePB) throws AnalysisException {
        stageType = stagePB.getType();
        stageId = stagePB.getStageId();
        ObjectStoreInfoPB objInfo = stagePB.getObjInfo();
        stagePrefix = objInfo.getPrefix();
        objectInfo = RemoteBase.analyzeStageObjectStoreInfo(stagePB);
        brokerProperties.put(S3Properties.Env.ENDPOINT, objInfo.getEndpoint());
        brokerProperties.put(S3Properties.Env.REGION, objInfo.getRegion());
        brokerProperties.put(S3Properties.Env.ACCESS_KEY, objectInfo.getAk());
        brokerProperties.put(S3Properties.Env.SECRET_KEY, objectInfo.getSk());
        if (objectInfo.getToken() != null) {
            brokerProperties.put(S3Properties.Env.TOKEN, objectInfo.getToken());
        }
        brokerProperties.put(S3_BUCKET, objInfo.getBucket());
        brokerProperties.put(S3_PREFIX, objInfo.getPrefix());
        brokerProperties.put(S3Properties.PROVIDER, objInfo.getProvider().toString());
        StageProperties stageProperties = new StageProperties(stagePB.getPropertiesMap());
        this.copyIntoProperties.mergeProperties(stageProperties);
        this.copyIntoProperties.analyze();
    }

    public String getDbName() {
        return label.getDbName();
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public List<DataDescription> getDataDescriptions() {
        return Lists.newArrayList(dataDescription);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public LabelName getLabel() {
        return label;
    }

    public long getSizeLimit() {
        return this.copyIntoProperties.getSizeLimit();
    }

    public boolean isAsync() {
        return this.copyIntoProperties.isAsync();
    }

    public boolean isForce() {
        return this.copyIntoProperties.isForce();
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ").append(tableName.toSql()).append(" \n");
        sb.append("from ").append(copyFromParam.toSql()).append("\n");
        if (copyIntoProperties != null && copyIntoProperties.getProperties().size() > 0) {
            sb.append(" PROPERTIES ").append(copyIntoProperties.toSql());
        }
        return sb.toString();
    }

    public ShowResultSetMetaData getMetaData() {
        return COPY_INTO_META_DATA;
    }

    public String getPattern() {
        return this.copyFromParam.getStageAndPattern().getPattern();
    }

    public Map<String, String> getOptHints() {
        return optHints;
    }
}
