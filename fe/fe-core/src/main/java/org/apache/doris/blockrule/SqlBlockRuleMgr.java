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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.gson.GsonUtils;

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

    public boolean existRule(String name) {
        return nameToSqlBlockRuleMap.containsKey(name);
    }

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

    public void createSqlBlockRule(CreateSqlBlockRuleStmt stmt) throws UserException {
        writeLock();
        try {
            SqlBlockRule sqlBlockRule = SqlBlockRule.fromCreateStmt(stmt);
            String ruleName = sqlBlockRule.getName();
            if (existRule(ruleName)) {
                throw new DdlException("the sql block rule " + ruleName + " already create");
            }
            unprotectedAdd(sqlBlockRule);
            Catalog.getCurrentCatalog().getEditLog().logCreateSqlBlockRule(sqlBlockRule);
        } finally {
            writeUnlock();
        }
    }

    public void replayCreate(SqlBlockRule sqlBlockRule) {
        unprotectedAdd(sqlBlockRule);
        LOG.info("replay create sql block rule: {}", sqlBlockRule);
    }

    public void alterSqlBlockRule(AlterSqlBlockRuleStmt stmt) throws AnalysisException, DdlException {
        writeLock();
        try {
            SqlBlockRule sqlBlockRule = SqlBlockRule.fromAlterStmt(stmt);
            String ruleName = sqlBlockRule.getName();
            if (!existRule(ruleName)) {
                throw new DdlException("the sql block rule " + ruleName + " not exist");
            }
            SqlBlockRule originRule = nameToSqlBlockRuleMap.get(ruleName);
            SqlBlockUtil.checkAlterValidate(sqlBlockRule, originRule);
            if (StringUtils.isEmpty(sqlBlockRule.getSql())) {
                sqlBlockRule.setSql(originRule.getSql());
            }
            if (StringUtils.isEmpty(sqlBlockRule.getSqlHash())) {
                sqlBlockRule.setSqlHash(originRule.getSqlHash());
            }
            if (StringUtils.isEmpty(sqlBlockRule.getPartitionNum().toString())) {
                sqlBlockRule.setPartitionNum(originRule.getPartitionNum());
            }
            if (StringUtils.isEmpty(sqlBlockRule.getTabletNum().toString())) {
                sqlBlockRule.setTabletNum(originRule.getTabletNum());
            }
            if (StringUtils.isEmpty(sqlBlockRule.getCardinality().toString())) {
                sqlBlockRule.setCardinality(originRule.getCardinality());
            }
            if (sqlBlockRule.getGlobal() == null) {
                sqlBlockRule.setGlobal(originRule.getGlobal());
            }
            if (sqlBlockRule.getEnable() == null) {
                sqlBlockRule.setEnable(originRule.getEnable());
            }
            unprotectedUpdate(sqlBlockRule);
            Catalog.getCurrentCatalog().getEditLog().logAlterSqlBlockRule(sqlBlockRule);
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
    }

    public void unprotectedAdd(SqlBlockRule sqlBlockRule) {
        nameToSqlBlockRuleMap.put(sqlBlockRule.getName(), sqlBlockRule);
    }

    public void dropSqlBlockRule(DropSqlBlockRuleStmt stmt) throws DdlException {
        writeLock();
        try {
            List<String> ruleNames = stmt.getRuleNames();
            for (String ruleName : ruleNames) {
                if (!existRule(ruleName)) {
                    throw new DdlException("the sql block rule " + ruleName + " not exist");
                }
            }
            unprotectedDrop(ruleNames);
            Catalog.getCurrentCatalog().getEditLog().logDropSqlBlockRule(ruleNames);
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

    public void matchSql(String originSql, String sqlHash, String user) throws AnalysisException {
        // match global rule
        List<SqlBlockRule> globalRules = nameToSqlBlockRuleMap.values().stream().filter(SqlBlockRule::getGlobal).collect(Collectors.toList());
        for (SqlBlockRule rule : globalRules) {
            matchSql(rule, originSql, sqlHash);
        }
        // match user rule
        String[] bindSqlBlockRules = Catalog.getCurrentCatalog().getAuth().getSqlBlockRules(user);
        for (String ruleName : bindSqlBlockRules) {
            SqlBlockRule rule = nameToSqlBlockRuleMap.get(ruleName);
            if (rule == null) {
                continue;
            }
            matchSql(rule, originSql, sqlHash);
        }
    }

    public void matchSql(SqlBlockRule rule, String originSql, String sqlHash) throws AnalysisException {
        if (rule.getEnable()) {
            if (StringUtils.isNotEmpty(rule.getSqlHash()) &&
                    (!CreateSqlBlockRuleStmt.STRING_NOT_SET.equals(rule.getSqlHash()) && rule.getSqlHash().equals(sqlHash))) {
                MetricRepo.COUNTER_HIT_SQL_BLOCK_RULE.increase(1L);
                throw new AnalysisException("sql match hash sql block rule: " + rule.getName());
            } else if (StringUtils.isNotEmpty(rule.getSql()) &&
                    (!CreateSqlBlockRuleStmt.STRING_NOT_SET.equals(rule.getSql()) && rule.getSqlPattern().matcher(originSql).find())) {
                MetricRepo.COUNTER_HIT_SQL_BLOCK_RULE.increase(1L);
                throw new AnalysisException("sql match regex sql block rule: " + rule.getName());
            }
        }
    }

    public void checkLimitaions(Long partitionNum, Long tabletNum, Long cardinality, String user) throws AnalysisException {
        // match global rule
        List<SqlBlockRule> globalRules = nameToSqlBlockRuleMap.values().stream().filter(SqlBlockRule::getGlobal).collect(Collectors.toList());
        for (SqlBlockRule rule : globalRules) {
            checkLimitaions(rule, partitionNum, tabletNum, cardinality);
        }
        // match user rule
        String[] bindSqlBlockRules = Catalog.getCurrentCatalog().getAuth().getSqlBlockRules(user);
        for (String ruleName : bindSqlBlockRules) {
            SqlBlockRule rule = nameToSqlBlockRuleMap.get(ruleName);
            if (rule == null) {
                continue;
            }
            checkLimitaions(rule, partitionNum, tabletNum, cardinality);
        }
    }

    public void checkLimitaions(SqlBlockRule rule, Long partitionNum, Long tabletNum, Long cardinality) throws AnalysisException {
        if (rule.getPartitionNum() == 0 && rule.getTabletNum() == 0 && rule.getCardinality() == 0) {
            return;
        } else if (rule.getEnable()) {
            if ((rule.getPartitionNum() != 0 && rule.getPartitionNum() < partitionNum)
                    || (rule.getTabletNum() != 0 && rule.getTabletNum() < tabletNum)
                    || (rule.getCardinality() != 0 && rule.getCardinality() < cardinality)) {
                MetricRepo.COUNTER_HIT_SQL_BLOCK_RULE.increase(1L);
                if (rule.getPartitionNum() < partitionNum) {
                    throw new AnalysisException("sql hits sql block rule: " + rule.getName() + ", reach partition_num : " + rule.getPartitionNum());
                } else if (rule.getTabletNum() < tabletNum) {
                    throw new AnalysisException("sql hits sql block rule: " + rule.getName() + ", reach tablet_num : " + rule.getTabletNum());
                } else if (rule.getCardinality() < cardinality) {
                    throw new AnalysisException("sql hits sql block rule: " + rule.getName() + ", reach cardinality : " + rule.getCardinality());
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
