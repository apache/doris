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

package org.apache.doris.blockrule;

import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.DropSqlBlockRuleStmt;
import org.apache.doris.analysis.ShowSqlBlockRuleStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manage SqlBlockRule.
 **/
public class SqlBlockRuleMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(SqlBlockRuleMgr.class);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "nameToSqlBlockRuleMap")
    private Map<String, SqlBlockRule> nameToSqlBlockRuleMap = Maps.newConcurrentMap();

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    /**
     * Judge whether exist rule by ruleName.
     **/
    public boolean existRule(String name) {
        return nameToSqlBlockRuleMap.containsKey(name);
    }

    /**
     * Get SqlBlockRule by show stmt.
     **/
    public List<SqlBlockRule> getSqlBlockRule(ShowSqlBlockRuleStmt stmt) throws AnalysisException {
        String ruleName = stmt.getRuleName();
        if (StringUtils.isNotEmpty(ruleName)) {
            if (nameToSqlBlockRuleMap.containsKey(ruleName)) {
                SqlBlockRule sqlBlockRule = nameToSqlBlockRuleMap.get(ruleName);
                return Lists.newArrayList(sqlBlockRule);
            }
            return Lists.newArrayList();
        }
        return Lists.newArrayList(nameToSqlBlockRuleMap.values());
    }

    /**
     * Check limitation's  effectiveness of a SqlBlockRule.
     **/
    private static void verifyLimitations(SqlBlockRule sqlBlockRule) throws DdlException {
        if (sqlBlockRule.getPartitionNum() < 0) {
            throw new DdlException("the value of partition_num can't be a negative");
        }
        if (sqlBlockRule.getTabletNum() < 0) {
            throw new DdlException("the value of tablet_num can't be a negative");
        }
        if (sqlBlockRule.getCardinality() < 0) {
            throw new DdlException("the value of cardinality can't be a negative");
        }
    }

    /**
     * Create SqlBlockRule for create stmt.
     **/
    public void createSqlBlockRule(CreateSqlBlockRuleStmt stmt) throws UserException {
        writeLock();
        try {
            SqlBlockRule sqlBlockRule = SqlBlockRule.fromCreateStmt(stmt);
            String ruleName = sqlBlockRule.getName();
            if (existRule(ruleName)) {
                if (stmt.isIfNotExists()) {
                    return;
                }
                throw new DdlException("the sql block rule " + ruleName + " already create");
            }
            verifyLimitations(sqlBlockRule);
            unprotectedAdd(sqlBlockRule);
            Env.getCurrentEnv().getEditLog().logCreateSqlBlockRule(sqlBlockRule);
        } finally {
            writeUnlock();
        }
    }

    /**
     * Add local cache when receive editLog.
     **/
    public void replayCreate(SqlBlockRule sqlBlockRule) {
        unprotectedAdd(sqlBlockRule);
        LOG.info("replay create sql block rule: {}", sqlBlockRule);
    }

    /**
     * Alter SqlBlockRule for alter stmt.
     **/
    public void alterSqlBlockRule(AlterSqlBlockRuleStmt stmt) throws AnalysisException, DdlException {
        writeLock();
        try {
            SqlBlockRule sqlBlockRule = SqlBlockRule.fromAlterStmt(stmt);
            String ruleName = sqlBlockRule.getName();
            if (!existRule(ruleName)) {
                throw new DdlException("the sql block rule " + ruleName + " not exist");
            }
            SqlBlockRule originRule = nameToSqlBlockRuleMap.get(ruleName);

            if (sqlBlockRule.getSql().equals(CreateSqlBlockRuleStmt.STRING_NOT_SET)) {
                sqlBlockRule.setSql(originRule.getSql());
            }
            if (sqlBlockRule.getSqlHash().equals(CreateSqlBlockRuleStmt.STRING_NOT_SET)) {
                sqlBlockRule.setSqlHash(originRule.getSqlHash());
            }
            if (sqlBlockRule.getPartitionNum().equals(AlterSqlBlockRuleStmt.LONG_NOT_SET)) {
                sqlBlockRule.setPartitionNum(originRule.getPartitionNum());
            }
            if (sqlBlockRule.getTabletNum().equals(AlterSqlBlockRuleStmt.LONG_NOT_SET)) {
                sqlBlockRule.setTabletNum(originRule.getTabletNum());
            }
            if (sqlBlockRule.getCardinality().equals(AlterSqlBlockRuleStmt.LONG_NOT_SET)) {
                sqlBlockRule.setCardinality(originRule.getCardinality());
            }
            if (sqlBlockRule.getGlobal() == null) {
                sqlBlockRule.setGlobal(originRule.getGlobal());
            }
            if (sqlBlockRule.getEnable() == null) {
                sqlBlockRule.setEnable(originRule.getEnable());
            }
            verifyLimitations(sqlBlockRule);
            SqlBlockUtil.checkAlterValidate(sqlBlockRule);

            unprotectedUpdate(sqlBlockRule);
            Env.getCurrentEnv().getEditLog().logAlterSqlBlockRule(sqlBlockRule);
        } finally {
            writeUnlock();
        }
    }

    public void replayAlter(SqlBlockRule sqlBlockRule) {
        unprotectedUpdate(sqlBlockRule);
        LOG.info("replay alter sql block rule: {}", sqlBlockRule);
    }

    private void unprotectedUpdate(SqlBlockRule sqlBlockRule) {
        nameToSqlBlockRuleMap.put(sqlBlockRule.getName(), sqlBlockRule);
    }

    private void unprotectedAdd(SqlBlockRule sqlBlockRule) {
        nameToSqlBlockRuleMap.put(sqlBlockRule.getName(), sqlBlockRule);
    }

    /**
     * Drop SqlBlockRule for drop stmt.
     **/
    public void dropSqlBlockRule(DropSqlBlockRuleStmt stmt) throws DdlException {
        writeLock();
        try {
            List<String> ruleNames = stmt.getRuleNames();
            for (String ruleName : ruleNames) {
                if (!existRule(ruleName)) {
                    if (stmt.isIfExists()) {
                        continue;
                    }
                    throw new DdlException("the sql block rule " + ruleName + " not exist");
                }
            }
            unprotectedDrop(ruleNames);
            Env.getCurrentEnv().getEditLog().logDropSqlBlockRule(ruleNames);
        } finally {
            writeUnlock();
        }
    }

    public void replayDrop(List<String> ruleNames) {
        unprotectedDrop(ruleNames);
        LOG.info("replay drop sql block ruleNames: {}", ruleNames);
    }

    public void unprotectedDrop(List<String> ruleNames) {
        ruleNames.forEach(name -> nameToSqlBlockRuleMap.remove(name));
    }

    /**
     * Match SQL according to rules.
     **/
    public void matchSql(String originSql, String sqlHash, String user) throws AnalysisException {
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable().internalSession) {
            return;
        }
        // match global rule
        List<SqlBlockRule> globalRules =
                nameToSqlBlockRuleMap.values().stream().filter(SqlBlockRule::getGlobal).collect(Collectors.toList());
        for (SqlBlockRule rule : globalRules) {
            matchSql(rule, originSql, sqlHash);
        }
        // match user rule
        String[] bindSqlBlockRules = Env.getCurrentEnv().getAuth().getSqlBlockRules(user);
        for (String ruleName : bindSqlBlockRules) {
            SqlBlockRule rule = nameToSqlBlockRuleMap.get(ruleName);
            if (rule == null) {
                continue;
            }
            matchSql(rule, originSql, sqlHash);
        }
    }

    private void matchSql(SqlBlockRule rule, String originSql, String sqlHash) throws AnalysisException {
        if (rule.getEnable()) {
            if (StringUtils.isNotEmpty(rule.getSqlHash()) && !SqlBlockUtil.STRING_DEFAULT.equals(rule.getSqlHash())
                    && rule.getSqlHash().equals(sqlHash)) {
                MetricRepo.COUNTER_HIT_SQL_BLOCK_RULE.increase(1L);
                throw new AnalysisException("sql match hash sql block rule: " + rule.getName());
            } else if (StringUtils.isNotEmpty(rule.getSql()) && !SqlBlockUtil.STRING_DEFAULT.equals(rule.getSql())
                    && rule.getSqlPattern() != null && rule.getSqlPattern().matcher(originSql).find()) {
                MetricRepo.COUNTER_HIT_SQL_BLOCK_RULE.increase(1L);
                throw new AnalysisException("sql match regex sql block rule: " + rule.getName());
            }
        }
    }

    /**
     * Check number whether legal by user.
     **/
    public void checkLimitations(Long partitionNum, Long tabletNum, Long cardinality, String user)
            throws AnalysisException {
        if (ConnectContext.get().getSessionVariable().internalSession) {
            return;
        }
        // match global rule
        for (SqlBlockRule rule : nameToSqlBlockRuleMap.values()) {
            if (rule.getGlobal()) {
                checkLimitations(rule, partitionNum, tabletNum, cardinality);
            }
        }
        // match user rule
        String[] bindSqlBlockRules = Env.getCurrentEnv().getAuth().getSqlBlockRules(user);
        for (String ruleName : bindSqlBlockRules) {
            SqlBlockRule rule = nameToSqlBlockRuleMap.get(ruleName);
            if (rule == null) {
                continue;
            }
            checkLimitations(rule, partitionNum, tabletNum, cardinality);
        }
    }

    /**
     * Check number whether legal by SqlBlockRule.
     **/
    private void checkLimitations(SqlBlockRule rule, Long partitionNum, Long tabletNum, Long cardinality)
            throws AnalysisException {
        if (rule.getPartitionNum() == 0 && rule.getTabletNum() == 0 && rule.getCardinality() == 0) {
            return;
        } else if (rule.getEnable()) {
            if ((rule.getPartitionNum() != 0 && rule.getPartitionNum() < partitionNum) || (rule.getTabletNum() != 0
                    && rule.getTabletNum() < tabletNum) || (rule.getCardinality() != 0
                    && rule.getCardinality() < cardinality)) {
                MetricRepo.COUNTER_HIT_SQL_BLOCK_RULE.increase(1L);
                if (rule.getPartitionNum() < partitionNum && rule.getPartitionNum() != 0) {
                    throw new AnalysisException(
                            "sql hits sql block rule: " + rule.getName() + ", reach partition_num : "
                                    + rule.getPartitionNum());
                } else if (rule.getTabletNum() < tabletNum && rule.getTabletNum() != 0) {
                    throw new AnalysisException("sql hits sql block rule: " + rule.getName() + ", reach tablet_num : "
                            + rule.getTabletNum());
                } else if (rule.getCardinality() < cardinality && rule.getCardinality() != 0) {
                    throw new AnalysisException("sql hits sql block rule: " + rule.getName() + ", reach cardinality : "
                            + rule.getCardinality());
                }
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static SqlBlockRuleMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, SqlBlockRuleMgr.class);
    }
}
