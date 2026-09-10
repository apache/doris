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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
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
import java.util.stream.Collectors;

/**
 * Show dictionaries command.
 */
public class ShowDictionariesCommand extends ShowCommand {
    private final String wild;
    private final PatternMatcher matcher;

    public ShowDictionariesCommand(String wild) throws AnalysisException {
        super(PlanType.SHOW_DICTIONARIES_COMMAND);
        this.wild = wild;
        this.matcher = PatternMatcherWrapper.createMysqlPattern(wild, true);
    }

    /**
     * get meta data
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("DictionaryId", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DictionaryName", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("BaseTableName", ScalarType.createVarchar(80)));
        builder.addColumn(new Column("Version", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Status", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("DataDistribution", ScalarType.createVarchar(1000)));
        builder.addColumn(new Column("LastUpdateResult", ScalarType.createVarchar(100)));
        return builder.build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws AnalysisException {
        List<List<String>> rows = Lists.newArrayList();

        DictionaryManager dictionaryManager = ctx.getEnv().getDictionaryManager();
        List<Dictionary> queryDicts = Lists.newArrayList();
        String dbName = ctx.getDatabase();
        // getDictionaries() already have read lock
        Map<String, Dictionary> dbDictionaries = dictionaryManager.getDictionaries(dbName);
        for (Map.Entry<String, Dictionary> entry : dbDictionaries.entrySet()) {
            String dictionaryName = entry.getKey();
            // Apply wild condition filtering if wild pattern is provided
            if (wild != null && !matcher.match(dictionaryName)) {
                continue;
            }
            // Dictionaries are authorized like tables of the internal catalog. Hide the ones the user
            // may not show, the same way SHOW TABLES hides tables, so the source table name, status
            // and data distribution are not exposed to users without privileges on the dictionary.
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME,
                    dbName, dictionaryName, PrivPredicate.SHOW)) {
                continue;
            }
            queryDicts.add(entry.getValue());
        }

        // An empty id list means "all dictionaries" to the BE status RPC (get_dictionary_status in
        // BackendService.thrift), so an empty visible set must return before status collection:
        // otherwise it fans RPCs to every alive BE for dictionaries this user may not see, logs
        // them as missing, and a bad response from any BE would fail the whole command.
        if (queryDicts.isEmpty()) {
            return new ShowResultSet(getMetaData(), rows);
        }

        // ignore its return value because we dont update it, just show.
        dictionaryManager
                .collectDictionaryStatus(queryDicts.stream().map(Dictionary::getId).collect(Collectors.toList()));

        for (Dictionary dictionary : queryDicts) {
            List<String> row = new ArrayList<>();
            row.add(String.valueOf(dictionary.getId())); // id
            row.add(dictionary.getName()); // name
            row.add(String.join(".", dictionary.getSourceQualifiedName())); // base table name
            row.add(String.valueOf(dictionary.getVersion())); // version
            row.add(dictionary.getStatus().name()); // status
            row.add(dictionary.prettyPrintDistributions()); // data distribution
            row.add(dictionary.getLastUpdateResult()); // last update result
            rows.add(row);
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        // Dictionary's status will not sync to follower. so we must forward the query to the master.
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowDictionariesCommand(this, context);
    }
}
