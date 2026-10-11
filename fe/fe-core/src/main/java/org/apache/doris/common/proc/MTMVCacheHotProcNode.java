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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVCacheManager.HotEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MTMVCacheHotProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("MtmvId").add("DbName").add("MvName").add("Guarded").add("IdleMs").build();

    private static final String UNKNOWN_DB = "<unknown>";
    private static final String DROPPED_MV = "<dropped>";

    private final MTMVCacheManager manager;

    public MTMVCacheHotProcNode(MTMVCacheManager manager) {
        this.manager = manager;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        List<HotEntry> entries = manager.hotEntries(Config.mtmv_cache_hot_show_num);
        if (entries.isEmpty()) {
            return result;
        }
        Set<Long> wanted = new HashSet<>();
        for (HotEntry e : entries) {
            wanted.add(e.mtmvId);
        }
        Map<Long, Table> idToTable = resolveTables(wanted);
        for (HotEntry entry : entries) {
            String dbName = UNKNOWN_DB;
            String mvName = DROPPED_MV;
            Table table = idToTable.get(entry.mtmvId);
            if (table != null) {
                mvName = table.getName();
                String qualified = table.getQualifiedDbName();
                if (qualified != null && !qualified.isEmpty()) {
                    dbName = qualified;
                }
            }
            result.addRow(Lists.newArrayList(
                    String.valueOf(entry.mtmvId),
                    dbName,
                    mvName,
                    entry.guarded ? "Yes" : "No",
                    String.valueOf(entry.idleMs)));
        }
        return result;
    }

    private static Map<Long, Table> resolveTables(Set<Long> ids) {
        Map<Long, Table> out = new HashMap<>();
        if (Env.getCurrentEnv() == null) {
            return out;
        }
        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        if (catalog == null) {
            return out;
        }
        for (Database db : catalog.getDbs()) {
            for (Long id : ids) {
                if (out.containsKey(id)) {
                    continue;
                }
                Table t = db.getTableNullable(id);
                if (t != null) {
                    out.put(id, t);
                }
            }
            if (out.size() == ids.size()) {
                break;
            }
        }
        return out;
    }
}
