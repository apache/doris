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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * rule hint.
 */
public class UseMvHint extends Hint {

    private final boolean isUseMv;

    private final boolean isAllMv;

    private final List<String> parameters;

    private final Map<String, String> useMvTableColumnMap;

    private final Map<String, List<String>> noUseMvTableColumnMap;

    /**
     * constructor of use mv hint
     * @param hintName use mv
     * @param parameters original parameters
     * @param isUseMv use_mv hint or no_use_mv hint
     * @param isAllMv should all mv be controlled
     */
    public UseMvHint(String hintName, List<String> parameters, boolean isUseMv, boolean isAllMv) {
        super(hintName);
        this.isUseMv = isUseMv;
        this.isAllMv = isAllMv;
        this.parameters = parameters;
        this.useMvTableColumnMap = initUseMvTableColumnMap(parameters);
        this.noUseMvTableColumnMap = initNoUseMvTableColumnMap(parameters);
    }

    private Map<String, String> initUseMvTableColumnMap(List<String> parameters) {
        Map<String, String> tempUseMvTableColumnMap = new HashMap<>();
        if (!isUseMv) {
            return tempUseMvTableColumnMap;
        }
        if (parameters.size() % 2 == 1) {
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("parameter of use_mv hint must be in pairs");
            return tempUseMvTableColumnMap;
        }
        for (int i = 0; i < parameters.size(); i += 2) {
            String tableName = parameters.get(i);
            String columnName = parameters.get(i + 1);
            if (tempUseMvTableColumnMap.containsKey(tableName)) {
                this.setStatus(HintStatus.SYNTAX_ERROR);
                this.setErrorMessage("use_mv hint should only have one mv in one table: "
                        + tableName + "." + columnName);
                break;
            }
            tempUseMvTableColumnMap.put(tableName, columnName);
        }
        return tempUseMvTableColumnMap;
    }

    private Map<String, List<String>> initNoUseMvTableColumnMap(List<String> parameters) {
        Map<String, List<String>> tempNoUseMvTableColumnMap = new HashMap<>();
        if (isUseMv) {
            return tempNoUseMvTableColumnMap;
        }
        if (parameters.size() % 2 == 1) {
            this.setStatus(HintStatus.SYNTAX_ERROR);
            this.setErrorMessage("parameter of no_use_mv hint must be in pairs");
            return tempNoUseMvTableColumnMap;
        }
        for (int i = 0; i < parameters.size(); i += 2) {
            String tableName = parameters.get(i);
            String columnName = parameters.get(i + 1);
            if (tempNoUseMvTableColumnMap.containsKey(tableName)) {
                tempNoUseMvTableColumnMap.get(tableName).add(columnName);
            } else {
                List<String> list = new ArrayList<>();
                list.add(columnName);
                tempNoUseMvTableColumnMap.put(tableName, list);
            }
        }
        return tempNoUseMvTableColumnMap;
    }

    public boolean isUseMv() {
        return isUseMv;
    }

    public boolean isAllMv() {
        return isAllMv;
    }

    public String getUseMvName(String tableName) {
        return useMvTableColumnMap.get(tableName);
    }

    public List<String> getNoUseMVName(String tableName) {
        return noUseMvTableColumnMap.get(tableName);
    }

    @Override
    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        if (isUseMv) {
            out.append("use_mv");
        } else {
            out.append("no_use_mv");
        }
        if (!parameters.isEmpty()) {
            out.append("(");
            for (int i = 0; i < parameters.size(); i++) {
                if (i % 2 == 0) {
                    out.append(parameters.get(i));
                } else {
                    out.append(".");
                    out.append(parameters.get(i));
                    out.append(" ");
                }
            }
            out.append(")");
        }

        return out.toString();
    }
}
