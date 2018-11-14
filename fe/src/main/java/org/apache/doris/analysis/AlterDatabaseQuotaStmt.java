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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

public class AlterDatabaseQuotaStmt extends DdlStmt {
    private String dbName;
    private String quotaExpression;
    private long quota;
    private String unit = "B";
    private HashMap<String, Long> validUnitMultiplier = new HashMap<String, Long>();
    private String quotaPattern = "(-?\\d+)(\\D*)";

    public AlterDatabaseQuotaStmt(String dbName, String quotaExpression) {
        this.dbName = dbName;
        this.quotaExpression = quotaExpression;
        validUnitMultiplier.put("B", 1L);
        validUnitMultiplier.put("Bytes", 1L);
        validUnitMultiplier.put("K", 1024L);
        validUnitMultiplier.put("KB", 1024L);
        validUnitMultiplier.put("M", 1024 * 1024L);
        validUnitMultiplier.put("MB", 1024 * 1024L);
        validUnitMultiplier.put("G", 1024 * 1024 * 1024L);
        validUnitMultiplier.put("GB", 1024 * 1024 * 1024L);
        validUnitMultiplier.put("T", 1024 * 1024 * 1024 * 1024L);
        validUnitMultiplier.put("TB", 1024 * 1024 * 1024 * 1024L);
        validUnitMultiplier.put("P", 1024 * 1024 * 1024 * 1024 * 1024L);
        validUnitMultiplier.put("PB", 1024 * 1024 * 1024 * 1024 * 1024L);
    }

    public String getDbName() {
        return dbName;
    }

    public long getQuota() {
        return quota;
    }

    public String getUnit() {
        return unit;
    }

    private void getQuotaFromExpression() throws UserException {
        Pattern r = Pattern.compile(quotaPattern);
        Matcher m = r.matcher(quotaExpression);
        if (m.find( )) {
            try {
                quota = Long.parseLong(m.group(1));
            } catch(NumberFormatException nfe) {
                throw new AnalysisException("invalid quota:" + m.group(1));
            }
            if (quota < 0L) {
                throw new AnalysisException("Quota must larger than 0");
            }

            String tmpUnit = m.group(2);
            if (!Strings.isNullOrEmpty(tmpUnit)) {
                unit = tmpUnit.toUpperCase();
            }
            if (validUnitMultiplier.containsKey(unit)) {
                quota = quota * validUnitMultiplier.get(unit);
            } else {
                throw new AnalysisException("invalid unit:" + tmpUnit);
            }
        } else {
            throw new AnalysisException("invalid quota expression:" + quotaExpression);
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getQualifiedUser(), dbName);
        }

        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        getQuotaFromExpression();
    }

    @Override
    public String toSql() {
        return "ALTER DATABASE " + dbName + " SET DATA QUOTA " + quotaExpression;
    }

}
