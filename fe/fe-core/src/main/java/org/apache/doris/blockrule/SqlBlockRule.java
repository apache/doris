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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Use for block some sql by rule.
 **/
public class SqlBlockRule implements Writable {

    public static final String NAME_TYPE = "SQL BLOCK RULE NAME";

    // the rule name, cluster unique
    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "sql")
    private String sql;

    // sql md5
    @SerializedName(value = "sqlHash")
    private String sqlHash;

    // partitionNum will be scanned
    @SerializedName(value = "partitionNum")
    private Long partitionNum;

    // tabletNum will be scanned
    @SerializedName(value = "tabletNum")
    private Long tabletNum;

    // cardinality
    @SerializedName(value = "cardinality")
    private Long cardinality;

    // whether effective global
    @SerializedName(value = "global")
    private Boolean global;

    // whether to use the rule
    @SerializedName(value = "enable")
    private Boolean enable;

    private Pattern sqlPattern;

    /**
     * Create SqlBlockRule.
     **/
    public SqlBlockRule(String name, String sql, String sqlHash, Long partitionNum, Long tabletNum, Long cardinality,
            Boolean global, Boolean enable) {
        this.name = name;
        this.sql = sql;
        this.sqlHash = sqlHash;
        this.partitionNum = partitionNum;
        this.tabletNum = tabletNum;
        this.cardinality = cardinality;
        this.global = global;
        this.enable = enable;
        if (StringUtils.isNotEmpty(sql)) {
            this.sqlPattern = Pattern.compile(sql);
        }
    }

    public static SqlBlockRule fromCreateStmt(CreateSqlBlockRuleStmt stmt) {
        return new SqlBlockRule(stmt.getRuleName(), stmt.getSql(), stmt.getSqlHash(), stmt.getPartitionNum(),
                stmt.getTabletNum(), stmt.getCardinality(), stmt.isGlobal(), stmt.isEnable());
    }

    public static SqlBlockRule fromAlterStmt(AlterSqlBlockRuleStmt stmt) {
        return new SqlBlockRule(stmt.getRuleName(), stmt.getSql(), stmt.getSqlHash(), stmt.getPartitionNum(),
                stmt.getTabletNum(), stmt.getCardinality(), stmt.getGlobal(), stmt.getEnable());
    }

    public String getName() {
        return name;
    }

    public String getSql() {
        return sql;
    }

    public Pattern getSqlPattern() {
        return sqlPattern;
    }

    public String getSqlHash() {
        return sqlHash;
    }

    public Long getPartitionNum() {
        return partitionNum;
    }

    public Long getTabletNum() {
        return tabletNum;
    }

    public Long getCardinality() {
        return cardinality;
    }

    public Boolean getGlobal() {
        return global;
    }

    public Boolean getEnable() {
        return enable;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setSqlPattern(Pattern sqlPattern) {
        this.sqlPattern = sqlPattern;
    }

    public void setSqlHash(String sqlHash) {
        this.sqlHash = sqlHash;
    }

    public void setPartitionNum(Long partitionNum) {
        this.partitionNum = partitionNum;
    }

    public void setTabletNum(Long tabletNum) {
        this.tabletNum = tabletNum;
    }

    public void setCardinality(Long cardinality) {
        this.cardinality = cardinality;
    }

    public void setGlobal(Boolean global) {
        this.global = global;
    }

    public void setEnable(Boolean enable) {
        this.enable = enable;
    }

    /**
     * Show SqlBlockRule info.
     **/
    public List<String> getShowInfo() {
        return Lists.newArrayList(this.name, this.sql, this.sqlHash,
                this.partitionNum == null ? "0" : Long.toString(this.partitionNum),
                this.tabletNum == null ? "0" : Long.toString(this.tabletNum),
                this.cardinality == null ? "0" : Long.toString(this.cardinality), String.valueOf(this.global),
                String.valueOf(this.enable));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    /**
     * Read data from file.
     **/
    public static SqlBlockRule read(DataInput in) throws IOException {
        String json = Text.readString(in);
        SqlBlockRule sqlBlockRule = GsonUtils.GSON.fromJson(json, SqlBlockRule.class);
        if (StringUtils.isNotEmpty(sqlBlockRule.getSql()) && !SqlBlockUtil.STRING_DEFAULT.equals(
                sqlBlockRule.getSql())) {
            sqlBlockRule.setSqlPattern(Pattern.compile(sqlBlockRule.getSql()));
        }
        return sqlBlockRule;
    }
}
