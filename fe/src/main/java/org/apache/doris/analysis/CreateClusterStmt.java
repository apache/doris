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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Map;

@Deprecated
public class CreateClusterStmt extends DdlStmt {
    public static String CLUSTER_INSTANCE_NUM = "instance_num";
    public static String CLUSTER_SUPERMAN_PASSWORD = "password";
    public static String CLUSTER_SUPERUSER_NAME = "superuser";

    private String clusterName;
    private boolean ifNotExists;
    private int instanceNum;
    private Map<String, String> properties;
    private byte[] scramblePassword;
    private String passwd;

    public CreateClusterStmt() {

    }

    public CreateClusterStmt(String clusterName, Map<String, String> properties, String passwd) {
        this.clusterName = clusterName;
        this.properties = properties;
        this.passwd = passwd;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "CREATE CLUSTER");
        }
        FeNameFormat.checkDbName(clusterName);
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_AUTHORITY, analyzer.getQualifiedUser());
        }

        if (properties == null || properties.size() == 0 || !properties.containsKey(CLUSTER_INSTANCE_NUM)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PARAMETER);
        }

        try {
            instanceNum = Integer.valueOf(properties.get(CLUSTER_INSTANCE_NUM));
        } catch (NumberFormatException e) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PARAMETER);
        }

        if (instanceNum < 0) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_CREATE_ISTANCE_NUM_ERROR);
        }
        
        final String password = passwd;
        if (!Strings.isNullOrEmpty(password)) {
            scramblePassword = MysqlPassword.makeScrambledPassword(password);
        } else {
            scramblePassword = new byte[0];
        }
    }

    @Override
    public String toSql() {
        final String sql = "CREATE CLUSTER " + clusterName + " PROPERTIES(\"instance_num\"=" + "\"" + instanceNum
                + "\")" + "IDENTIFIED BY '" + passwd + "'";
        return sql;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public void setInstanceNum(int instanceNum) {
        this.instanceNum = instanceNum;
    }

    public byte[] getPassword() {
        return scramblePassword;
    }

}
