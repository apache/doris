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

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.StageProperties;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.datasource.CloudInternalCatalog;
import org.apache.doris.cloud.proto.Cloud.GetIamResponse;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.cloud.proto.Cloud.RamUserPB;
import org.apache.doris.cloud.proto.Cloud.StagePB;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageAccessType;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.storage.RemoteBase;
import org.apache.doris.cloud.storage.RemoteBase.ObjectInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

/**
 * CreateStageCommand
 */
public class CreateStageCommand extends Command implements ForwardWithSync, NeedAuditEncryption {
    private static final Logger LOG = LogManager.getLogger(CreateStageCommand.class);
    protected StagePB.StageType type;
    private final boolean ifNotExists;
    private final String stageName;
    private StageProperties stageProperties;

    public CreateStageCommand(boolean ifNotExists, String stageName, Map<String, String> properties) {
        super(PlanType.CREATE_STAGE_COMMAND);
        this.ifNotExists = ifNotExists;
        this.stageName = stageName;
        this.stageProperties = new StageProperties(properties);
        this.type = StagePB.StageType.EXTERNAL;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateStageCommand(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        ((CloudEnv) Env.getCurrentEnv()).createStage(this);
    }

    /**
     * validate
     */
    public void validate() throws UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check stage name
        FeNameFormat.checkResourceName(stageName, ResourceTypeEnum.STAGE);
        stageProperties.analyze();
        checkObjectStorageInfo();
    }

    private void checkObjectStorageInfo() throws UserException {
        RemoteBase remote = null;
        try {
            tryConnect(stageProperties.getEndpoint());
            StagePB stagePB = toStageProto();
            if (stagePB.getAccessType() == StageAccessType.IAM
                    || stagePB.getAccessType() == StageAccessType.BUCKET_ACL) {
                GetIamResponse iamUsers = ((CloudInternalCatalog) Env.getCurrentInternalCatalog()).getIam();
                RamUserPB user;
                if (stagePB.getAccessType() == StageAccessType.BUCKET_ACL) {
                    if (!iamUsers.hasRamUser()) {
                        throw new AnalysisException("Instance does not have ram user");
                    }
                    user = iamUsers.getRamUser();
                } else {
                    user = iamUsers.getIamUser();
                }
                ObjectStoreInfoPB objInfoPB = ObjectStoreInfoPB.newBuilder(stagePB.getObjInfo()).setAk(user.getAk())
                        .setSk(user.getSk()).build();
                stagePB = StagePB.newBuilder(stagePB).setExternalId(user.getExternalId()).setObjInfo(objInfoPB).build();
            }

            ObjectInfo objectInfo = RemoteBase.analyzeStageObjectStoreInfo(stagePB);
            remote = RemoteBase.newInstance(objectInfo);

            // RemoteBase#headObject does not throw exception if key does not exist.
            remote.headObject("1");
            remote.listObjects(null);
        } catch (Exception e) {
            LOG.warn("Failed to access object storage, proto={}, err={}",
                    stageProperties.getObjectStoreInfoPB(), e.toString());
            String msg;
            if (e instanceof UserException) {
                msg = ((UserException) e).getDetailMessage();
            } else {
                msg = e.getMessage();
            }
            throw new UserException(InternalErrorCode.GET_REMOTE_DATA_ERROR,
                    "Failed to access object storage, message=" + msg, e);
        } finally {
            if (remote != null) {
                remote.close();
            }
        }
    }

    private void tryConnect(String endpoint) throws Exception {
        HttpURLConnection connection = null;
        try {
            String urlStr = "http://" + endpoint;
            // TODO: Server-Side Request Forgery Check is still need?
            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(10000);
            connection.connect();
        } catch (SocketTimeoutException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Failed to connect endpoint=" + endpoint, e);
            throw e;
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception e) {
                    LOG.warn("Failed to disconnect connection, endpoint={}", endpoint, e);
                }
            }
        }
    }

    /**
     * StagePB
     */
    public StagePB toStageProto() throws DdlException {
        StagePB.Builder stageBuilder = StagePB.newBuilder();
        // external stage doesn't need username
        stageBuilder.setStageId(UUID.randomUUID().toString());
        switch (type) {
            case EXTERNAL:
                stageBuilder.setName(getStageName()).setType(StageType.EXTERNAL)
                        .setObjInfo(stageProperties.getObjectStoreInfoPB()).setComment(stageProperties.getComment())
                        .setCreateTime(System.currentTimeMillis()).setAccessType(stageProperties.getAccessType());
                break;
            case INTERNAL:
            default:
                throw new DdlException("Can not create stage with type=" + type);
        }

        stageBuilder.putAllProperties(stageProperties.getDefaultProperties());
        if (stageBuilder.getAccessType() == StageAccessType.IAM) {
            stageBuilder.setRoleName(stageProperties.getRoleName()).setArn(stageProperties.getArn());
        }
        return stageBuilder.build();
    }

    public boolean isDryRun() {
        return stageProperties.isDryRun();
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getStageName() {
        return stageName;
    }

    public StageProperties getStageProperties() {
        return stageProperties;
    }
}
