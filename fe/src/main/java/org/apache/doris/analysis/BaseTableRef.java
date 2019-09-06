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

import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An actual table, such as OLAP table or a MySQL table.
 * BaseTableRef.
 */
public class BaseTableRef extends TableRef {
    private static final Logger LOG = LogManager.getLogger(BaseTableRef.class);

    private Table table;

    public BaseTableRef(TableRef ref, Table table, TableName tableName) {
        super(ref);
        this.table = table;
        this.name = tableName;
        // Set implicit aliases if no explicit one was given.
        if (hasExplicitAlias()) return;
        aliases_ = new String[] { name.toString(), tableName.getNoClusterString(), table.getName() };
    }

    protected BaseTableRef(BaseTableRef other) {
        super(other);
        name = other.name;
        table = other.table;
    }

    @Override
    public TableRef clone() {
        return new BaseTableRef(this);
    }

    @Override
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) {
        TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
        result.setTable(table);
        return result;
    }

    /**
     * Register this table ref and then analyze the Join clause.
     */
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        name = analyzer.getFqTableName(name);
        name.analyze(analyzer);
        desc = analyzer.registerTableRef(this);
        isAnalyzed = true;  // true that we have assigned desc
        analyzeJoin(analyzer);
        analyzeSortHints();
        analyzeHints();
    }
}

