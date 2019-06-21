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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

public class AlterDatabaseQuotaStmt extends DdlStmt {
    private String dbName;
    private String quotaQuantity;
    private long quota;
    private static ImmutableMap<String, Long> validUnitMultiplier = 
        ImmutableMap.<String, Long>builder().put("B", 1L)
        .put("K", 1024L)
        .put("KB", 1024L)
        .put("M", 1024L * 1024)
        .put("MB", 1024L * 1024)
        .put("G", 1024L * 1024 * 1024)
        .put("GB", 1024L * 1024 * 1024)
        .put("T", 1024L * 1024 * 1024 * 1024)
        .put("TB", 1024L * 1024 * 1024 * 1024)
        .put("P", 1024L * 1024 * 1024 * 1024 * 1024)
        .put("PB", 1024L * 1024 * 1024 * 1024 * 1024).build();

    private String quotaPattern = "(\\d+)(\\D*)";

    public AlterDatabaseQuotaStmt(String dbName, String quotaQuantity) {
        this.dbName = dbName;
        this.quotaQuantity = quotaQuantity;
    }

    public String getDbName() {
        return dbName;
    }

    public long getQuota() {
        return quota;
    }

    private void analyzeQuotaQuantity() throws UserException {
        Pattern r = Pattern.compile(quotaPattern);
        Matcher m = r.matcher(quotaQuantity);
        if (m.matches()) {
            try {
                quota = Long.parseLong(m.group(1));
            } catch(NumberFormatException nfe) {
                throw new AnalysisException("invalid quota:" + m.group(1));
            }
            if (quota < 0L) {
                throw new AnalysisException("Quota must larger than 0");
            }

            String unit = "B";
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
            throw new AnalysisException("invalid quota expression:" + quotaQuantity);
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
        analyzeQuotaQuantity();
    }

    @Override
    public String toSql() {
        return "ALTER DATABASE " + dbName + " SET DATA QUOTA " + quotaQuantity;
    }
}
