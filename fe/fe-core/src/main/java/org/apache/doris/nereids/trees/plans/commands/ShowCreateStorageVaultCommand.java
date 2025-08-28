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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ShowCreateStorageVaultCommand
 */
public class ShowCreateStorageVaultCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
            .addColumn(new Column("StorageVaultName", ScalarType.createVarchar(128)))
            .addColumn(new Column("Create Storage Vault", ScalarType.createVarchar(65535)))
            .build();

    private final String storageVaultName;

    public ShowCreateStorageVaultCommand(String storageVaultName) {
        super(PlanType.SHOW_CREATE_STORAGE_VAULT_COMMAND);
        this.storageVaultName = storageVaultName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();

        List<List<String>> rows = Lists.newArrayList();
        try {
            Cloud.GetObjStoreInfoResponse response = MetaServiceProxy.getInstance()
                    .getObjStoreInfo(Cloud.GetObjStoreInfoRequest.newBuilder().build());
            AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
            UserIdentity user = ctx.getCurrentUserIdentity();

            Optional<Cloud.StorageVaultPB> storageVaultPB = response.getStorageVaultList().stream()
                    .filter(storageVault -> storageVault.getName().equals(storageVaultName))
                    .filter(storageVault -> accessManager.checkStorageVaultPriv(user, storageVault.getName(),
                        PrivPredicate.USAGE))
                    .findFirst();

            Preconditions.checkArgument(storageVaultPB.isPresent(), "storageVaultPB is null");

            String createStmt = "";
            if (storageVaultPB.get().hasHdfsInfo()) {
                createStmt = getHdfsCreateStmt(storageVaultPB.get().getHdfsInfo());
            }
            if (storageVaultPB.get().hasObjInfo()) {
                createStmt = getObjectCreateStmt(storageVaultPB.get().getObjInfo());
            }
            rows.add(Arrays.asList(storageVaultName, createStmt));
        } catch (RpcException e) {
            throw new AnalysisException(e.getMessage());
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    private void validate() throws AnalysisException {
        if (Config.isNotCloudMode()) {
            throw new AnalysisException("Storage Vault is only supported for cloud mode");
        }
        if (!FeConstants.runningUnitTest) {
            // In legacy cloud mode, some s3 back-ended storage does need to use storage vault.
            if (!((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
                throw new AnalysisException("Your cloud instance doesn't support storage vault");
            }
        }
    }

    private String getObjectCreateStmt(Cloud.ObjectStoreInfoPB objectInfo) {
        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("CREATE STORAGE VAULT ");
        stmtBuilder.append(storageVaultName);
        stmtBuilder.append("\nPROPERTIES(\n");

        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "S3");
        properties.put("s3.endpoint", objectInfo.getEndpoint());
        properties.put("s3.region", objectInfo.getRegion());
        properties.put("s3.root.path", objectInfo.getPrefix());
        properties.put("s3.bucket", objectInfo.getBucket());
        properties.put("s3.access_key", objectInfo.getAk());
        properties.put("s3.secret_key", objectInfo.getSk());
        properties.put("provider", objectInfo.getProvider().name());
        properties.put("use_path_style", String.valueOf(objectInfo.getUsePathStyle()));
        if (objectInfo.hasExternalEndpoint()) {
            properties.put("s3.external_endpoint", objectInfo.getExternalEndpoint());
        }

        stmtBuilder.append(new PrintableMap<>(properties, " = ", true, true, true));
        stmtBuilder.append(")\n");

        return stmtBuilder.toString();
    }

    private String getHdfsCreateStmt(Cloud.HdfsVaultInfo hdfsInfo) {
        Cloud.HdfsBuildConf buildConf = hdfsInfo.getBuildConf();
        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("CREATE STORAGE VAULT ");
        stmtBuilder.append(storageVaultName);
        stmtBuilder.append("\nPROPERTIES(\n");

        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hdfs");
        properties.put("fs.defaultFS", buildConf.getFsName());
        properties.put("path_prefix", hdfsInfo.getPrefix());
        if (buildConf.hasUser()) {
            properties.put("hadoop.username", buildConf.getUser());
        }
        if (buildConf.hasHdfsKerberosPrincipal()) {
            properties.put("hadoop.kerberos.principal", buildConf.getHdfsKerberosPrincipal());
        }
        if (buildConf.hasHdfsKerberosKeytab()) {
            properties.put("hadoop.kerberos.keytab", buildConf.getHdfsKerberosKeytab());
        }

        buildConf.getHdfsConfsList().stream()
                .map(hdfsConfKVPair -> properties.put(hdfsConfKVPair.getKey(), hdfsConfKVPair.getValue()));

        stmtBuilder.append(new PrintableMap<>(properties, " = ", true, true, true));
        stmtBuilder.append(")\n");

        return stmtBuilder.toString();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateStorageVaultCommand(this, context);
    }
}
