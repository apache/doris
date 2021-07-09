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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class SqlBlockRule implements Writable {

    public static final String NAME_TYPE = "SQL BLOCK RULE NAME";

    public static final String DEFAULT_USER = "default";

    // the rule name, cluster unique
    private String name;

    // default stands for all users
    private String user;

    private String sql;

    // whether to use the rule
    private Boolean enable;

    public SqlBlockRule(String name) {
        this.name = name;
    }

    public SqlBlockRule(String name, String user, String sql,  Boolean enable) {
        this.name = name;
        this.user = user;
        this.sql = sql;
        this.enable = enable;
    }

    public static SqlBlockRule fromCreateStmt(CreateSqlBlockRuleStmt stmt) {
        return new SqlBlockRule(stmt.getRuleName(), stmt.getUser(), stmt.getSql(), stmt.isEnable());
    }

    public static SqlBlockRule fromAlterStmt(AlterSqlBlockRuleStmt stmt) {
        return new SqlBlockRule(stmt.getRuleName(), stmt.getUser(), stmt.getSql(), stmt.getEnable());
    }

    public String getName() {
        return name;
    }

    public String getUser() {
        return user;
    }

    public String getSql() {
        return sql;
    }

    public Boolean getEnable() {
        return enable;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setEnable(Boolean enable) {
        this.enable = enable;
    }

    public List<String> getShowInfo() {
        return Lists.newArrayList(this.name, this.user, this.sql, String.valueOf(this.enable));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
        Text.writeString(out, user);
        Text.writeString(out, sql);
        Text.writeString(out, String.valueOf(enable));
    }

    public static SqlBlockRule read(DataInput in) throws IOException {
        return new SqlBlockRule(Text.readString(in), Text.readString(in), Text.readString(in), Boolean.valueOf(Text.readString(in)));
    }
}
