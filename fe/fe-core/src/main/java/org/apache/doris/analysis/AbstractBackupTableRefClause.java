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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class AbstractBackupTableRefClause implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(AbstractBackupTableRefClause.class);

    private boolean isExclude;
    private List<TableRef> tableRefList;

    public AbstractBackupTableRefClause(boolean isExclude, List<TableRef> tableRefList) {
        this.isExclude = isExclude;
        this.tableRefList = tableRefList;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // normalize
        // table name => table ref
        Map<String, TableRef> tblPartsMap;
        if (GlobalVariable.lowerCaseTableNames == 0) {
            // comparisons case sensitive
            tblPartsMap = Maps.newTreeMap();
        } else {
            tblPartsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        }
        for (TableRef tblRef : tableRefList) {
            String tblName = tblRef.getName().getTbl();
            if (!tblPartsMap.containsKey(tblName)) {
                tblPartsMap.put(tblName, tblRef);
            } else {
                throw new AnalysisException("Duplicated table: " + tblName);
            }
        }

        // update table ref
        tableRefList.clear();
        for (TableRef tableRef : tblPartsMap.values()) {
            tableRefList.add(tableRef);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("table refs after normalization: {}", Joiner.on(",").join(tableRefList));
        }
    }

    public boolean isExclude() {
        return isExclude;
    }

    public List<TableRef> getTableRefList() {
        return tableRefList;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (isExclude) {
            sb.append("EXCLUDE ");
        } else {
            sb.append("ON ");
        }
        sb.append("\n(");
        sb.append(Joiner.on(",\n").join(tableRefList));
        sb.append("\n)");
        return sb.toString();
    }
}
