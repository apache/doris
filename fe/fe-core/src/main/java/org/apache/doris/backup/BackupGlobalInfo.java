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

package org.apache.doris.backup;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.blockrule.SqlBlockRule;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.mysql.privilege.CatalogPrivEntry;
import org.apache.doris.mysql.privilege.ColPrivilegeKey;
import org.apache.doris.mysql.privilege.DbPrivEntry;
import org.apache.doris.mysql.privilege.GlobalPrivEntry;
import org.apache.doris.mysql.privilege.PasswordPolicy;
import org.apache.doris.mysql.privilege.PrivEntry;
import org.apache.doris.mysql.privilege.ResourcePrivEntry;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.mysql.privilege.TablePrivEntry;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.mysql.privilege.WorkloadGroupPrivEntry;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.RowPolicy;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BackupGlobalInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);
    // Users
    @SerializedName(value = "users")
    private List<User> userList = Lists.newArrayList();
    // roles
    @SerializedName(value = "roles")
    private List<Role> roleList = Lists.newArrayList();
    @SerializedName(value = "userProperties")
    private List<UserProperty> userProperties = Lists.newArrayList();
    @SerializedName(value = "roleToUsers")
    private Map<String, Set<UserIdentity>> roleToUsers = Maps.newHashMap();
    @SerializedName(value = "policyMap")
    private Map<UserIdentity, PasswordPolicy> policyMap = Maps.newHashMap();
    // rowPolicy name -> policy
    @SerializedName(value = "rowPolicies")
    private List<Policy> rowPolicies = Lists.newArrayList();
    @SerializedName(value = "sqlBlockRules")
    private List<SqlBlockRule> sqlBlockRules = Lists.newArrayList();
    @SerializedName(value = "catalogs")
    private List<BackupCatalogMeta> catalogs = Lists.newArrayList();
    @SerializedName(value = "workloadGroups")
    private List<WorkloadGroup> workloadGroups = Lists.newArrayList();
    @SerializedName(value = "sqls")
    private String sqls = null;

    public BackupGlobalInfo() {
    }

    public List<User> getUserList() {
        return userList;
    }

    public List<Role> getRoleList() {
        return roleList;
    }

    public boolean isEmpty() {
        return StringUtils.isEmpty(sqls);
    }

    public List<WorkloadGroup> getWorkloadGroups() {
        return workloadGroups;
    }

    public String getSqls() {
        return sqls;
    }

    public List<BackupCatalogMeta> getCatalogs() {
        return catalogs;
    }

    public List<SqlBlockRule> getSqlBlockRules() {
        return sqlBlockRules;
    }

    public void genPrivSqls(StringBuffer sqlBuffer) {
        for (User user : userList) {
            sqlBuffer.append("create user if not exists " + user.getUserIdentity().toString()
                    + " identified by password '"
                    + new String(user.getPassword().getPassword()) + "' comment "
                    + (user.getComment().isEmpty() ? "''" : ("\"" + user.getComment() + "\"")) + ";");
            sqlBuffer.append("\n");
        }
        for (Role role : roleList) {
            String grantToStr = "";
            if (Role.isDefaultRoleName(role.getRoleName())) {
                String userIdentity = Role.getUserFromDefaultRole(role.getRoleName());
                String[] parts = userIdentity.split("@");
                String user = parts[0];
                String host = parts[1];
                grantToStr = "'" + user + "'@'" + host + "'";
            } else {
                sqlBuffer.append("create role if not exists " + role.getRoleName() + ";");
                sqlBuffer.append("\n");
                grantToStr = "ROLE '" + role.getRoleName() + "'";
            }

            // ==============GlobalPrivs==============
            for (PrivEntry entry : role.getGlobalPrivTable().getEntries()) {
                GlobalPrivEntry tEntry = (GlobalPrivEntry) entry;
                if (tEntry.getPrivSet().isEmpty()) {
                    continue;
                }
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on *.*.* TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            // ============== CatalogPrivs ========================
            for (PrivEntry entry : role.getCatalogPrivTable().getEntries()) {
                CatalogPrivEntry tEntry = (CatalogPrivEntry) entry;

                if (tEntry.getPrivSet().isEmpty()) {
                    continue;
                }
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on `" + tEntry.getOrigCtl()
                        + "`.*.* TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            // databasePrivs
            for (PrivEntry entry : role.getDbPrivTable().getEntries()) {
                DbPrivEntry tEntry = (DbPrivEntry) entry;

                if (tEntry.getPrivSet().isEmpty()) {
                    continue;
                }
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on " + "`" + tEntry.getOrigCtl() + "`.`"
                        + tEntry.getOrigDb() + "`.* TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            // tbl
            for (PrivEntry entry : role.getTablePrivTable().getEntries()) {
                TablePrivEntry tEntry = (TablePrivEntry) entry;

                if (tEntry.getPrivSet().isEmpty()) {
                    continue;
                }
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on " + "`" + tEntry.getOrigCtl() + "`.`"
                        + tEntry.getOrigDb()  + "`.`" + tEntry.getOrigTbl()
                        + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            // col
            for (Map.Entry<ColPrivilegeKey, Set<String>> entry : role.getColPrivMap().entrySet()) {
                String colPrivStr = String.format("%s(%s)", entry.getKey().getPrivilege(),
                        String.join(",", entry.getValue()));
                sqlBuffer.append("grant " + colPrivStr + " on " + "`" + entry.getKey().getCtl() + "`.`"
                        + entry.getKey().getDb()  + "`.`" + entry.getKey().getTbl()
                        + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            //ResourcePrivs
            for (PrivEntry entry : role.getResourcePrivTable().getEntries()) {
                ResourcePrivEntry tEntry = (ResourcePrivEntry) entry;
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on RESOURCE `" + tEntry.getOrigResource()
                        + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            //CloudClusterPrivs
            for (PrivEntry entry : role.getCloudClusterPrivTable().getEntries()) {
                ResourcePrivEntry tEntry = (ResourcePrivEntry) entry;
                // convertResourcePrivToCloudPriv
                sqlBuffer.append("grant " + tEntry.getPrivSet().toString().replace("Cluster_usage_priv", "usage_priv")
                        + " on CLUSTER `" + tEntry.getOrigResource() + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            //CloudStagePrivs
            for (PrivEntry entry : role.getCloudStagePrivTable().getEntries()) {
                ResourcePrivEntry tEntry = (ResourcePrivEntry) entry;
                // convertResourcePrivToCloudPriv
                sqlBuffer.append("grant " + tEntry.getPrivSet().toString().replace("Stage_usage_priv", "usage_priv")
                        + " on STAGE `" + tEntry.getOrigResource() + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            //StorageVaultPrivs
            for (PrivEntry entry : role.getStorageVaultPrivTable().getEntries()) {
                ResourcePrivEntry tEntry = (ResourcePrivEntry) entry;
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on STORAGE VAULT `" + tEntry.getOrigResource()
                        + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            //WorkloadGroupPrivs
            for (PrivEntry entry : role.getWorkloadGroupPrivTable().getEntries()) {
                WorkloadGroupPrivEntry tEntry = (WorkloadGroupPrivEntry) entry;
                sqlBuffer.append("grant " + tEntry.getPrivSet() + " on WORKLOAD GROUP  `"
                        + tEntry.getOrigWorkloadGroupName()
                        + "` TO " + grantToStr + ";");
                sqlBuffer.append("\n");
            }
            //ComputeGroupPrivs are same as CloudClusterPrivs
        }
        for (UserProperty userProperty : userProperties) {
            List<List<String>> list = userProperty.fetchProperty();
            for (List<String> row : list) {
                String key = row.get(0);
                String value = row.get(1);
                if (StringUtils.isEmpty(value)) {
                    continue;
                }
                sqlBuffer.append("set property for '" + userProperty.getQualifiedUser()
                        + "' '" + key + "' = '" + value + "';");
                sqlBuffer.append("\n");
            }
        }
        for (Map.Entry<String, Set<UserIdentity>> entry : roleToUsers.entrySet()) {
            for (UserIdentity userIdentity : entry.getValue()) {
                if (Role.isDefaultRoleName(entry.getKey())) {
                    continue;
                }
                sqlBuffer.append("grant '" + entry.getKey() + "' to " + userIdentity.toString() + ";");
                sqlBuffer.append("\n");
            }
        }

        for (Map.Entry<UserIdentity, PasswordPolicy> entry : policyMap.entrySet()) {
            PasswordPolicy passwordPolicy = entry.getValue();
            if (passwordPolicy.getHistoryPolicy().historyNum == PasswordPolicy.HistoryPolicy.DEFAULT) {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_HISTORY DEFAULT;");
            } else {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_HISTORY "
                        + passwordPolicy.getHistoryPolicy().historyNum + ";");
            }
            sqlBuffer.append("\n");
            if (passwordPolicy.getExpirePolicy().expirationSecond == PasswordPolicy.ExpirePolicy.DEFAULT) {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_EXPIRE DEFAULT;");
            } else if (passwordPolicy.getExpirePolicy().expirationSecond == PasswordPolicy.ExpirePolicy.NEVER) {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_EXPIRE NEVER;");
            } else {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_EXPIRE INTERVAL "
                        + passwordPolicy.getExpirePolicy().expirationSecond + " SECOND;");
            }
            sqlBuffer.append("\n");
            if (passwordPolicy.getFailedLoginPolicy().passwordLockSeconds
                    == PasswordPolicy.FailedLoginPolicy.UNBOUNDED) {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_LOCK_TIME UNBOUNDED;");
            } else {
                sqlBuffer.append("alter user " + entry.getKey().toString() + " PASSWORD_LOCK_TIME "
                        + passwordPolicy.getFailedLoginPolicy().passwordLockSeconds + " SECOND;");
            }
            sqlBuffer.append("\n");
            sqlBuffer.append("alter user " + entry.getKey().toString() + " FAILED_LOGIN_ATTEMPTS "
                    + passwordPolicy.getFailedLoginPolicy().numFailedLogin + ";");
            sqlBuffer.append("\n");
        }

        for (Policy policy : rowPolicies) {
            RowPolicy rowPolicy = (RowPolicy) policy;
            sqlBuffer.append("CREATE ROW POLICY " + rowPolicy.getPolicyIdent() + " ON " + "`" + rowPolicy.getCtlName()
                    + "`.`"  + rowPolicy.getDbName() + "`.`" + rowPolicy.getTableName() + "`" + " AS "
                    + rowPolicy.getFilterType().name() + " TO " + (rowPolicy.getUser() != null
                    ? rowPolicy.getUser().toString() : (" ROLE " + rowPolicy.getRoleName()))
                    + " USING " + rowPolicy.getWherePredicate().toSql() + ";");
            sqlBuffer.append("\n");
        }

        for (SqlBlockRule sqlBlockRule : sqlBlockRules) {
            StringBuffer properties = new StringBuffer();
            properties.append("\"global\" = \"" +  String.valueOf(sqlBlockRule.getGlobal()) + "\"");
            properties.append(", \"enable\" = \"" +  String.valueOf(sqlBlockRule.getEnable()) + "\"");
            if (sqlBlockRule.getSql() != null) {
                properties.append(", \"sql\" = \"" + sqlBlockRule.getSql() + "\"");
            }
            if (StringUtils.isNotEmpty(sqlBlockRule.getSqlHash()) && !sqlBlockRule.getSqlHash().equals("NULL")) {
                properties.append(", \"sql_hash\" = \"" + sqlBlockRule.getSqlHash() + "\"");
            }
            if (sqlBlockRule.getPartitionNum() != 0) {
                properties.append(", \"partition_num\" = \""
                        + Long.toString(sqlBlockRule.getPartitionNum()) + "\"");
            }
            if (sqlBlockRule.getTabletNum() != 0) {
                properties.append(", \"tablet_num\" = \"" + Long.toString(sqlBlockRule.getTabletNum()) + "\"");
            }
            if (sqlBlockRule.getCardinality() != 0) {
                properties.append(", \"cardinality\" = \"" + Long.toString(sqlBlockRule.getCardinality()) + "\"");
            }
            String sql = "create sql_block_rule if not exists " + sqlBlockRule.getName()
                    + " properties(" + properties.toString() + ");";
            sqlBuffer.append(sql);
            sqlBuffer.append("\n");
        }
    }

    public void genCatalogSqls(StringBuffer sqlBuffer) {
        try {
            for (BackupCatalogMeta catalog : catalogs) {
                List<List<String>> rows =
                        Env.getCurrentEnv().getCatalogMgr().showCreateCatalog(catalog.getCatalogName());
                if (rows.size() == 1) {
                    String sql = rows.get(0).get(1);
                    sqlBuffer.append(sql);
                    sqlBuffer.append("\n");
                }
            }
        } catch (Exception e) {
            LOG.warn("genCatalogSqls failed", e);
        }
    }

    public void genWorkloadGroupSqls(StringBuffer sqlBuffer) {
        for (WorkloadGroup workloadGroup : workloadGroups) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE ");
            sb.append("WORKLOAD GROUP `").append(workloadGroup.getName()).append("` PROPERTIES( ");
            sb.append(new PrintableMap<>(workloadGroup.getProperties(), " = ", true, false)).append(");");

            sqlBuffer.append(sb.toString());
            sqlBuffer.append("\n");
        }
    }

    public void init(boolean backupPriv, boolean backupCatalog, boolean backupWorkloadGroup) {
        StringBuffer sqlBuffer = new StringBuffer();

        if (backupCatalog) {
            catalogs = Env.getCurrentEnv().getCatalogMgr().getAllCatalogsCopied();
            genCatalogSqls(sqlBuffer);
        }

        if (backupWorkloadGroup) {
            workloadGroups = Env.getCurrentEnv().getWorkloadGroupMgr().getAllWorkloadGroupsCopied();
            genWorkloadGroupSqls(sqlBuffer);
        }

        if (backupPriv) {
            Env.getCurrentEnv().getAuth().getAuthInfoCopied(userList, roleList, userProperties, roleToUsers, policyMap);
            rowPolicies = Env.getCurrentEnv().getPolicyMgr().getCopiedPoliciesByType(PolicyTypeEnum.ROW);
            sqlBlockRules = Env.getCurrentEnv().getSqlBlockRuleMgr().getAllRulesCopied();
            genPrivSqls(sqlBuffer);
        }
        sqls = sqlBuffer.toString();

        LOG.info("Backup global info sqls: {}", sqls);
    }

    public static BackupGlobalInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, BackupGlobalInfo.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void writeToFile(File globalInfoFile) throws FileNotFoundException {
        PrintWriter printWriter = new PrintWriter(globalInfoFile);
        try {
            printWriter.print(sqls);
            printWriter.flush();
        } finally {
            printWriter.close();
        }
    }

}
