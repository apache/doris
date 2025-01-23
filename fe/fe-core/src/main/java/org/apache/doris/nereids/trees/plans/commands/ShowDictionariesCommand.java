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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Show dictionaries command.
 */
public class ShowDictionariesCommand extends ShowCommand {
    public ShowDictionariesCommand() {
        super(PlanType.SHOW_DICTIONARIES_COMMAND);
    }

    private ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("DictionaryId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DictionaryName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("BaseTableName", ScalarType.createVarchar(80)));
        builder.addColumn(new Column("Version", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Status", ScalarType.createVarchar(30)));
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) {
        List<List<String>> rows = Lists.newArrayList();

        DictionaryManager dictionaryManager = ctx.getEnv().getDictionaryManager();
        Map<String, Dictionary> dictionaries = dictionaryManager.getDictionaries(ctx.getDatabase());
        for (Map.Entry<String, Dictionary> entry : dictionaries.entrySet()) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(entry.getValue().getId())); // id
            row.add(entry.getKey()); // name
            row.add(String.join(".", entry.getValue().getSourceQualifiedName())); // base table name
            row.add(String.valueOf(entry.getValue().getVersion())); // version
            row.add(entry.getValue().getStatus().name()); // status
            rows.add(row);
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowDictionariesCommand(this, context);
    }
}
