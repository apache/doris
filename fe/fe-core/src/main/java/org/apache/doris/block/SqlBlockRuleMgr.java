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

package org.apache.doris.block;

import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.DropSqlBlockRuleStmt;
import org.apache.doris.analysis.ShowSqlBlockRuleStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SqlBlockRuleMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(SqlBlockRuleMgr.class);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private Map<String, List<SqlBlockRule>> userToSqlBlockRuleMap = Maps.newConcurrentMap();

    private Map<String, SqlBlockRule> nameToSqlBlockRuleMap = Maps.newConcurrentMap();

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public boolean existRule(String name) {
        return nameToSqlBlockRuleMap.containsKey(name);
    }

    public List<SqlBlockRule> get(ShowSqlBlockRuleStmt stmt) throws AnalysisException {
        String ruleName = stmt.getRuleName();
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        if (StringUtils.isNotEmpty(ruleName)) {
            if (nameToSqlBlockRuleMap.containsKey(ruleName)) {
                SqlBlockRule sqlBlockRule = nameToSqlBlockRuleMap.get(ruleName);
                return Lists.newArrayList(sqlBlockRule);
            }
            return Lists.newArrayList();
        }
        return Lists.newArrayList(nameToSqlBlockRuleMap.values());
    }

    public void createSqlBlockRule(CreateSqlBlockRuleStmt stmt) throws UserException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
        }
        SqlBlockRule sqlBlockRule = SqlBlockRule.fromCreateStmt(stmt);
        create(sqlBlockRule);
    }

    private void create(SqlBlockRule sqlBlockRule) throws DdlException {
        writeLock();
        try {
            String ruleName = sqlBlockRule.getName();
            if (existRule(ruleName)) {
                throw new DdlException("the sql block rule " + ruleName + " already create");
            }
            Catalog.getCurrentCatalog().getEditLog().logCreateSqlBlockRule(sqlBlockRule);
            unprotectedAdd(sqlBlockRule);
        } finally {
            writeUnlock();
        }
    }

    public void replayCreate(SqlBlockRule sqlBlockRule) {
        unprotectedAdd(sqlBlockRule);
        LOG.info("replay create sql block rule: {}", sqlBlockRule);
    }

    public void alterSqlBlockRule(AlterSqlBlockRuleStmt stmt) throws UserException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
        }
        SqlBlockRule sqlBlockRule = SqlBlockRule.fromAlterStmt(stmt);
        alter(sqlBlockRule);
    }

    private void alter(SqlBlockRule sqlBlockRule) throws DdlException {
        writeLock();
        try {
            String ruleName = sqlBlockRule.getName();
            if (!existRule(ruleName)) {
                throw new DdlException("the sql block rule " + ruleName + " not exist");
            }
            SqlBlockRule originRule = nameToSqlBlockRuleMap.get(ruleName);
            if (StringUtils.isEmpty(sqlBlockRule.getUser())) {
                sqlBlockRule.setUser(originRule.getUser());
            }
            if (StringUtils.isEmpty(sqlBlockRule.getSql())) {
                sqlBlockRule.setSql(originRule.getSql());
            }
            if (sqlBlockRule.getEnable() == null) {
                sqlBlockRule.setEnable(originRule.getEnable());
            }
            Catalog.getCurrentCatalog().getEditLog().logAlterSqlBlockRule(sqlBlockRule);
            unprotectedUpdate(sqlBlockRule);
        } finally {
            writeUnlock();
        }
    }

    public void replayAlter(SqlBlockRule sqlBlockRule) {
        unprotectedUpdate(sqlBlockRule);
        LOG.info("replay alter sql block rule: {}", sqlBlockRule);
    }

    public void unprotectedUpdate(SqlBlockRule sqlBlockRule) {
        nameToSqlBlockRuleMap.put(sqlBlockRule.getName(), sqlBlockRule);
        List<SqlBlockRule> sqlBlockRules = userToSqlBlockRuleMap.getOrDefault(sqlBlockRule.getUser(), new ArrayList<>());
        sqlBlockRules.removeIf(rule -> sqlBlockRule.getName().equals(rule.getName()));
        sqlBlockRules.add(sqlBlockRule);
        userToSqlBlockRuleMap.put(sqlBlockRule.getUser(), sqlBlockRules);
    }

    public void unprotectedAdd(SqlBlockRule sqlBlockRule) {
        nameToSqlBlockRuleMap.put(sqlBlockRule.getName(), sqlBlockRule);
        List<SqlBlockRule> sqlBlockRules = userToSqlBlockRuleMap.getOrDefault(sqlBlockRule.getUser(), new ArrayList<>());
        sqlBlockRules.add(sqlBlockRule);
        userToSqlBlockRuleMap.put(sqlBlockRule.getUser(), sqlBlockRules);
    }

    public void dropSqlBlockRule(DropSqlBlockRuleStmt stmt) throws UserException {
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)
                && !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
        }
        List<String> ruleNames = stmt.getRuleNames();
        drop(ruleNames);
    }

    public void drop(List<String> ruleNames) throws DdlException {
        writeLock();
        try {
            for (String ruleName : ruleNames) {
                if (!existRule(ruleName)) {
                    throw new DdlException("the sql block rule " + ruleName + " not exist");
                }
                SqlBlockRule sqlBlockRule = nameToSqlBlockRuleMap.get(ruleName);
                if (sqlBlockRule == null) {
                    continue;
                }
                Catalog.getCurrentCatalog().getEditLog().logDropSqlBlockRule(sqlBlockRule);
                unprotectedDrop(sqlBlockRule);
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayDrop(SqlBlockRule sqlBlockRule) {
        unprotectedDrop(sqlBlockRule);
        LOG.info("replay drop sql block rule: {}", sqlBlockRule);
    }

    public void unprotectedDrop(SqlBlockRule sqlBlockRule) {
        nameToSqlBlockRuleMap.remove(sqlBlockRule.getName());
        List<SqlBlockRule> sqlBlockRules = userToSqlBlockRuleMap.get(sqlBlockRule.getUser());
        sqlBlockRules.removeIf(rule -> sqlBlockRule.getName().equals(rule.getName()));
        userToSqlBlockRuleMap.put(sqlBlockRule.getUser(), sqlBlockRules);
    }

    public Map<String, List<SqlBlockRule>> getUserToSqlBlockRuleMap() {
        return userToSqlBlockRuleMap;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(nameToSqlBlockRuleMap.size());
        for (SqlBlockRule sqlBlockRule : nameToSqlBlockRuleMap.values()) {
            sqlBlockRule.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            SqlBlockRule read = SqlBlockRule.read(in);
            unprotectedAdd(read);
        }
    }
}
