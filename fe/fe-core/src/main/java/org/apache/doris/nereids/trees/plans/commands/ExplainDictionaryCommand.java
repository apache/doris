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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.DdlException;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Command for describing dictionary. The syntax is: DESCRIBE DICTIONARY dictionary_name.
 * The result format is just like ShowCommand. so use ShowCommand as base class rather than adding a new ExplainCommand.
 */
public class ExplainDictionaryCommand extends ShowCommand {
    private String dbName;
    private String dictionaryName;

    public ExplainDictionaryCommand(String dbName, String dicName) {
        super(PlanType.EXPLAIN_DICTIONARY_COMMAND);
        this.dbName = dbName;
        this.dictionaryName = dicName;
    }

    /**
     * get meta data
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Field", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Type", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Null", ScalarType.createVarchar(4)));
        builder.addColumn(new Column("Key", ScalarType.createVarchar(6)));
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws DdlException {
        List<List<String>> rows = Lists.newArrayList();

        DictionaryManager dictionaryManager = ctx.getEnv().getDictionaryManager();
        String db = dbName == null ? ctx.getDatabase() : dbName;
        Dictionary dictionary = dictionaryManager.getDictionary(db, dictionaryName);

        for (DictionaryColumnDefinition column : dictionary.getDicColumns()) {
            List<String> row = new ArrayList<>();
            row.add(column.getName());
            row.add(column.getType().toSql());
            row.add(column.isNullable() ? "YES" : "NO");
            row.add(column.isKey() ? "true" : "false");
            rows.add(row);
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExplainDictionaryCommand(this, context);
    }
}
