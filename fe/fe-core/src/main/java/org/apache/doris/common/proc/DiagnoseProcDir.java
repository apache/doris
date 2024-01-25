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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * show proc "/diagnose";
 */
public class DiagnoseProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Item").add("ErrorNum").add("WarningNum").build();

    enum DiagnoseStatus {
        OK,
        WARNING,
        ERROR,
    }

    static class DiagnoseItem {
        public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
                .add("Item").add("Status").add("Content").add("Detail Cmd").add("Suggestion").build();

        public String name = "";
        public DiagnoseStatus status = DiagnoseStatus.OK;
        public String content = "";
        public String detailCmd = "";
        public String suggestion = "";

        public List<String> toRow() {
            return Lists.newArrayList(name, status != null ? status.name().toUpperCase() : "", content,
                    detailCmd, suggestion);
        }
    }

    static class SubProcDir implements ProcDirInterface {
        public List<DiagnoseItem> getDiagnoseResult() {
            return null;
        }

        @Override
        public boolean register(String name, ProcNodeInterface node) {
            return false;
        }

        @Override
        public ProcNodeInterface lookup(String name) throws AnalysisException {
            return null;
        }

        @Override
        public ProcResult fetchResult() throws AnalysisException {
            List<List<String>> rows = getDiagnoseResult().stream().map(DiagnoseItem::toRow)
                    .collect(Collectors.toList());
            return new BaseProcResult(DiagnoseItem.TITLE_NAMES, rows);
        }
    }

    private Map<String, SubProcDir> subDiagnoses;

    DiagnoseProcDir() {
        subDiagnoses = Maps.newHashMap();
        subDiagnoses.put("cluster_balance", new DiagnoseClusterBalanceProcDir());

        // (TODO)
        //subDiagnoses.put("transactions", new DiagnoseTransactionsProcDir());
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        return subDiagnoses.get(name);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<List<String>> rows = subDiagnoses.entrySet().stream()
                .sorted(Comparator.comparing(it -> it.getKey()))
                .map(it -> {
                    List<DiagnoseItem> items = it.getValue().getDiagnoseResult();
                    long errNum = items.stream().filter(item -> item.status == DiagnoseStatus.ERROR).count();
                    long warningNum = items.stream().filter(item -> item.status == DiagnoseStatus.WARNING).count();
                    return Lists.newArrayList(it.getKey(), String.valueOf(errNum), String.valueOf(warningNum));
                })
                .collect(Collectors.toList());

        long totalErrNum = rows.stream().mapToLong(row -> Long.valueOf(row.get(1))).sum();
        long totalWarningNum = rows.stream().mapToLong(row -> Long.valueOf(row.get(2))).sum();
        rows.add(Lists.newArrayList("Total", String.valueOf(totalErrNum), String.valueOf(totalWarningNum)));

        return new BaseProcResult(TITLE_NAMES, rows);
    }
}
