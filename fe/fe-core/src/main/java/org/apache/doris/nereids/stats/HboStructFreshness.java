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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Freshness of a hbo struct info entry.
 *
 * <p>A hbo entry records the visible version of every table it was collected from (the
 * {@code S{table,...,vN}} token of the canonical struct info). A new load bumps that visible version,
 * which changes the fingerprint of the struct info, so an obsolete entry is never reused - it simply
 * stops matching, and it keeps occupying a cache slot. This helper compares the recorded versions
 * with the current ones so that {@code HBO SHOW STATISTICS} can report the state of an entry and so
 * that obsolete entries can be deleted.
 */
public class HboStructFreshness {
    /** The recorded version of every table is still the visible version. */
    public static final String STATE_LIVE = "live";
    /** At least one table has a newer visible version, or was dropped or replaced. */
    public static final String STATE_STALE = "stale";
    /** The recorded tables cannot be resolved (e.g. the entry was written by another cluster). */
    public static final String STATE_UNKNOWN = "unknown";

    private static final Logger LOG = LogManager.getLogger(HboStructFreshness.class);

    private final String state;
    private final String recordedTables;

    private HboStructFreshness(String state, String recordedTables) {
        this.state = state;
        this.recordedTables = recordedTables;
    }

    public String getState() {
        return state;
    }

    public boolean isStale() {
        return STATE_STALE.equals(state);
    }

    /** The tables and versions the entry was collected from, e.g. {@code hbo_test.t1:v2;hbo_test.t2:v3}. */
    public String getRecordedTables() {
        return recordedTables;
    }

    /**
     * Compare the versions recorded in {@code canonicalStructInfo} with the current visible versions.
     * An empty struct info (an entry without a struct literal, e.g. {@code HBO SET LEARNED STATISTICS}
     * without {@code STRUCT}) cannot be checked and is reported as unknown.
     */
    public static HboStructFreshness of(String canonicalStructInfo) {
        List<String[]> scans = parseScans(canonicalStructInfo);
        if (scans.isEmpty()) {
            return new HboStructFreshness(STATE_UNKNOWN, "");
        }
        boolean stale = false;
        boolean unknown = false;
        for (String[] scan : scans) {
            Long currentVersion = visibleVersionOf(scan[0]);
            if (currentVersion == null) {
                unknown = true;
            } else if (currentVersion > Long.parseLong(scan[1])) {
                stale = true;
            }
        }
        return new HboStructFreshness(stale ? STATE_STALE : unknown ? STATE_UNKNOWN : STATE_LIVE,
                renderRecordedTables(scans));
    }

    /** Split a canonical struct info into [tableName, recordedVisibleVersion] pairs. */
    private static List<String[]> parseScans(String canonicalStructInfo) {
        List<String[]> scans = new ArrayList<>();
        if (canonicalStructInfo == null || canonicalStructInfo.isEmpty()) {
            return scans;
        }
        int index = 0;
        while ((index = canonicalStructInfo.indexOf("S{", index)) >= 0) {
            int end = canonicalStructInfo.indexOf('}', index);
            if (end < 0) {
                break;
            }
            String[] parts = canonicalStructInfo.substring(index + 2, end).split(",");
            for (String part : parts) {
                if (part.startsWith("v")) {
                    scans.add(new String[] {parts[0], part.substring(1)});
                    break;
                }
            }
            index = end;
        }
        return scans;
    }

    private static String renderRecordedTables(List<String[]> scans) {
        Set<String> tables = new LinkedHashSet<>();
        for (String[] scan : scans) {
            tables.add(scan[0] + ":v" + scan[1]);
        }
        return String.join(";", tables);
    }

    /** The current visible version of {@code fullName} ({@code [catalog.]db.table}), or null. */
    private static Long visibleVersionOf(String fullName) {
        String[] parts = fullName.split("\\.");
        try {
            if (parts.length < 2 || parts.length > 3) {
                return null;
            }
            String catalogName = parts.length == 3 ? parts[0] : null;
            String dbName = parts[parts.length - 2];
            String tableName = parts[parts.length - 1];
            CatalogIf<?> catalog = catalogName == null
                    ? Env.getCurrentEnv().getCurrentCatalog()
                    : Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName.toLowerCase(Locale.ROOT));
            if (catalog == null) {
                return null;
            }
            DatabaseIf<?> database = catalog.getDbNullable(dbName);
            if (database == null) {
                return null;
            }
            TableIf table = database.getTableNullable(tableName);
            if (!(table instanceof OlapTable)) {
                return null;
            }
            return ((OlapTable) table).getVisibleVersion();
        } catch (RpcException | RuntimeException e) {
            // cloud mode resolves the visible version over rpc; an entry whose version cannot be
            // read is reported as unknown instead of failing the SHOW statement
            LOG.debug("failed to resolve the visible version of {} for a hbo entry", fullName, e);
            return null;
        }
    }
}
