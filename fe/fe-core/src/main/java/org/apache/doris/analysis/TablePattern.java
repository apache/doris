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

import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Three-segment-format: catalog.database.table. If the lower segment is specific,
 * the higher segment can't be a wildcard. The following examples are not allowed:
 * "ctl1.*.table1", "*.*.table2", "*.db1.*", ...
 */
public class TablePattern implements Writable, GsonPostProcessable {
    @SerializedName(value = "ctl")
    private String ctl;
    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "tbl")
    private String tbl;
    boolean isAnalyzed = false;

    public static TablePattern ALL;

    static {
        ALL = new TablePattern("*", "*", "*");
        try {
            ALL.analyze();
        } catch (AnalysisException e) {
            // will not happen
        }
    }

    private TablePattern() {
    }

    public TablePattern(String ctl, String db, String tbl) {
        this.ctl = Strings.isNullOrEmpty(ctl) ? "*" : ctl;
        this.db = Strings.isNullOrEmpty(db) ? "*" : db;
        this.tbl = Strings.isNullOrEmpty(tbl) ? "*" : tbl;
    }

    public TablePattern(String db, String tbl) {
        this.ctl = null;
        this.db = Strings.isNullOrEmpty(db) ? "*" : db;
        this.tbl = Strings.isNullOrEmpty(tbl) ? "*" : tbl;
    }

    public String getQualifiedCtl() {
        Preconditions.checkState(isAnalyzed);
        return ctl;
    }

    public String getQualifiedDb() {
        Preconditions.checkState(isAnalyzed);
        return db;
    }

    public String getTbl() {
        return tbl;
    }

    public PrivLevel getPrivLevel() {
        Preconditions.checkState(isAnalyzed);
        if (ctl.equals("*")) {
            return PrivLevel.GLOBAL;
        } else if (db.equals("*")) {
            return PrivLevel.CATALOG;
        } else if (tbl.equals("*")) {
            return PrivLevel.DATABASE;
        } else {
            return PrivLevel.TABLE;
        }
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (ctl == null) {
            analyze(analyzer.getDefaultCatalog());
        } else {
            analyze(ctl);
        }
    }

    private void analyze(String catalogName) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        this.ctl = Strings.isNullOrEmpty(catalogName) ? InternalCatalog.INTERNAL_CATALOG_NAME : catalogName;
        if ((!tbl.equals("*") && (db.equals("*") || ctl.equals("*")))
                || (!db.equals("*") && ctl.equals("*"))) {
            throw new AnalysisException("Do not support format: " + toString());
        }

        if (!ctl.equals("*")) {
            FeNameFormat.checkCatalogName(ctl);
        }

        if (!db.equals("*")) {
            FeNameFormat.checkDbName(db);
        }

        if (!tbl.equals("*")) {
            FeNameFormat.checkTableName(tbl);
        }
        isAnalyzed = true;
    }

    public void analyze() throws AnalysisException {
        analyze(ctl);
    }

    private void removeClusterPrefix() {
        if (db != null) {
            db = ClusterNamespace.getNameFromFullName(db);
        }
    }

    public static TablePattern read(DataInput in) throws IOException {
        TablePattern tablePattern;
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_111) {
            tablePattern = GsonUtils.GSON.fromJson(Text.readString(in), TablePattern.class);
        } else {
            String ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
            String db = Text.readString(in);
            String tbl = Text.readString(in);
            tablePattern = new TablePattern(ctl, db, tbl);
        }
        tablePattern.isAnalyzed = true;
        return tablePattern;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TablePattern)) {
            return false;
        }
        TablePattern other = (TablePattern) obj;
        return ctl.equals(other.getQualifiedCtl()) && db.equals(other.getQualifiedDb()) && tbl.equals(other.getTbl());
    }

    @Override
    public int hashCode() {
        return Stream.of(ctl, db, tbl).filter(Objects::nonNull)
                .map(String::hashCode)
                .reduce(17, (acc, h) -> 31 * acc + h);
    }

    @Override
    public String toString() {
        return Stream.of(ctl, db, tbl).filter(Objects::nonNull).collect(Collectors.joining("."));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkState(isAnalyzed);
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        removeClusterPrefix();
        isAnalyzed = true;
    }
}
