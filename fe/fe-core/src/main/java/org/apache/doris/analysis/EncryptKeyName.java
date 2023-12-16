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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class EncryptKeyName implements Writable {
    private static final Logger LOG = LogManager.getLogger(EncryptKeyName.class);

    @SerializedName(value = "db")
    private String db;
    @SerializedName(value = "keyName")
    private String keyName;

    public EncryptKeyName(String db, String keyName) {
        this.db = db;
        this.keyName = keyName.toLowerCase();
        if (db != null) {
            this.db = db.toLowerCase();
        }
    }

    public EncryptKeyName(String keyName) {
        this.db = null;
        this.keyName = keyName.toLowerCase();
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        FeNameFormat.checkCommonName("EncryptKey", keyName);
        if (db == null) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    public String getDb() {
        return db;
    }

    public String getKeyName() {
        return keyName;
    }

    @Override
    public String toString() {
        if (db == null) {
            return keyName;
        }
        return ClusterNamespace.getNameFromFullName(db) + "." + keyName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static EncryptKeyName read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, EncryptKeyName.class);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EncryptKeyName)) {
            return false;
        }
        EncryptKeyName o = (EncryptKeyName) obj;
        if ((db == null || o.db == null) && (db != o.db)) {
            if (db == null && o.db != null) {
                return false;
            }
            if (db != null && o.db == null) {
                return false;
            }
            if (!db.equalsIgnoreCase(o.db)) {
                return false;
            }
        }
        return keyName.equalsIgnoreCase(o.keyName);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(db) + Objects.hashCode(keyName);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("KEY ");
        if (db != null) {
            sb.append(ClusterNamespace.getNameFromFullName(db)).append(".");
        }
        sb.append(keyName);
        return sb.toString();
    }
}
