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

package org.apache.doris.nereids.hint;

import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * rule hint.
 */
public class UseMvHint extends Hint {

    private final boolean isUseMv;

    private final boolean isAllMv;

    private final List<List<String>> tables;

    private Map<List<String>, Boolean> useMvTableColumnMap = new HashMap<>();

    private Map<List<String>, Boolean> noUseMvTableColumnMap = new HashMap<>();

    /**
     * constructor of use mv hint
     * @param hintName use mv
     * @param tables original parameters
     * @param isUseMv use_mv hint or no_use_mv hint
     * @param isAllMv should all mv be controlled
     */
    public UseMvHint(String hintName, List<List<String>> tables, boolean isUseMv, boolean isAllMv, List<Hint> hints) {
        super(hintName);
        this.isUseMv = isUseMv;
        this.isAllMv = isAllMv;
        this.tables = tables;
        this.useMvTableColumnMap = initMvTableColumnMap(tables, true);
        this.noUseMvTableColumnMap = initMvTableColumnMap(tables, false);
        checkConflicts(hints);
    }

    public Map<List<String>, Boolean> getNoUseMvTableColumnMap() {
        return noUseMvTableColumnMap;
    }

    public Map<List<String>, Boolean> getUseMvTableColumnMap() {
        return useMvTableColumnMap;
    }

    private void checkConflicts(List<Hint> hints) {
        String otherUseMv = isUseMv ? "NO_USE_MV" : "USE_MV";
        Optional<UseMvHint> otherUseMvHint = Optional.empty();
        for (Hint hint : hints) {
            if (hint.getHintName().equals(otherUseMv)) {
                otherUseMvHint = Optional.of((UseMvHint) hint);
            }
        }
        if (!otherUseMvHint.isPresent()) {
            return;
        }
        Map<List<String>, Boolean> otherUseMvTableColumnMap = isUseMv
                ? otherUseMvHint.get().getNoUseMvTableColumnMap() : otherUseMvHint.get().getUseMvTableColumnMap();
        Map<List<String>, Boolean> thisUseMvTableColumnMap = isUseMv ? useMvTableColumnMap : noUseMvTableColumnMap;
        for (Map.Entry<List<String>, Boolean> entry : thisUseMvTableColumnMap.entrySet()) {
            List<String> mv = entry.getKey();
            if (otherUseMvTableColumnMap.get(mv) != null) {
                String errorMsg = "conflict mv exist in use_mv and no_use_mv in the same time. Mv name: "
                        + mv;
                super.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                super.setErrorMessage(errorMsg);
                otherUseMvHint.get().setStatus(Hint.HintStatus.SYNTAX_ERROR);
                otherUseMvHint.get().setErrorMessage(errorMsg);
            }
        }
    }

    private Map<List<String>, Boolean> initMvTableColumnMap(List<List<String>> parameters, boolean initUseMv) {
        Map<List<String>, Boolean> mvTableColumnMap;
        if (initUseMv && !isUseMv) {
            return useMvTableColumnMap;
        } else if (!initUseMv && isUseMv) {
            return noUseMvTableColumnMap;
        } else if (initUseMv && isUseMv) {
            mvTableColumnMap = useMvTableColumnMap;
        } else {
            mvTableColumnMap = noUseMvTableColumnMap;
        }
        for (List<String> table : parameters) {
            // materialize view qualifier should have length between 1 and 4
            // which 1 and 3 represent of async materialize view, 2 and 4 represent of sync materialize view
            // number of parameters          meaning
            // 1                             async materialize view, mvName
            // 2                             sync materialize view, tableName.mvName
            // 3                             async materialize view, catalogName.dbName.mvName
            // 3                             sync materialize view, catalogName.dbName.tableName.mvName
            if (table.size() < 1 || table.size() > 4) {
                this.setStatus(HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("parameters number of no_use_mv hint must between 1 and 4");
                return mvTableColumnMap;
            }
            String mvName = table.get(table.size() - 1);
            if (mvName.equals("`*`") && isUseMv) {
                this.setStatus(Hint.HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("use_mv hint should only have one mv in one table");
                return mvTableColumnMap;
            }
            List<String> dbQualifier = new ArrayList<>();
            if (table.size() == 3 || table.size() == 4) {
                mvTableColumnMap.put(table, false);
                return mvTableColumnMap;
            }
            CatalogIf catalogIf = ConnectContext.get().getCurrentCatalog();
            if (catalogIf == null) {
                this.setStatus(HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("Current catalog is not set.");
                return mvTableColumnMap;
            }
            String catalogName = catalogIf.getName();
            String dbName = ConnectContext.get().getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                this.setStatus(HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("Current database is not set.");
                return mvTableColumnMap;
            }
            dbQualifier.add(catalogName);
            dbQualifier.add(dbName);
            if (table.size() == 2) {
                dbQualifier.add(table.get(0));
            }
            dbQualifier.add(mvName);
            if (mvTableColumnMap.containsKey(dbQualifier)) {
                this.setStatus(HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("repeated parameters in use_mv hint: " + dbQualifier);
                return mvTableColumnMap;
            } else {
                mvTableColumnMap.put(dbQualifier, false);
            }
        }
        return mvTableColumnMap;
    }

    public boolean isUseMv() {
        return isUseMv;
    }

    public boolean isAllMv() {
        return isAllMv;
    }

    @Override
    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        if (isUseMv) {
            out.append("use_mv");
        } else {
            out.append("no_use_mv");
        }
        if (!tables.isEmpty()) {
            out.append("(");
            for (int i = 0; i < tables.size(); i++) {
                if (i % 2 == 0) {
                    out.append(tables.get(i));
                } else {
                    out.append(".");
                    out.append(tables.get(i));
                    out.append(" ");
                }
            }
            out.append(")");
        }

        return out.toString();
    }
}
