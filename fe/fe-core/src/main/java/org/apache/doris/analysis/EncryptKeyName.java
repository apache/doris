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

import com.google.common.base.Strings;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class EncryptKeyName implements Writable {
    private static final Logger LOG = LogManager.getLogger(EncryptKeyName.class);

    private String db;
    private String keyName;

    private EncryptKeyName() {

    }

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

    // used to analyze db element in function name, add cluster
    public String analyzeDb(Analyzer analyzer) throws AnalysisException {
        if (db == null) {
            db = analyzer.getDefaultDb();
        } else {
            if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
            }
            db = ClusterNamespace.getFullName(analyzer.getClusterName(), db);
        }
        return db;
    }

    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (keyName.length() == 0) {
            throw new AnalysisException("EncryptKey name can not be empty.");
        }
        for (int i = 0; i < keyName.length(); ++i) {
            if (!isValidCharacter(keyName.charAt(i))) {
                throw new AnalysisException(
                        "EncryptKey names must be all alphanumeric or underscore. " +
                                "Invalid name: " + keyName);
            }
        }
        if (Character.isDigit(keyName.charAt(0))) {
            throw new AnalysisException("EncryptKey cannot start with a digit: " + keyName);
        }
        if (db == null) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
            }
            db = ClusterNamespace.getFullName(analyzer.getClusterName(), db);
        }
    }

    private boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getKeyName() {
        return keyName;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    @Override
    public String toString() {
        if (db == null) {
            return keyName;
        }
        return db + "." + keyName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (db != null) {
            out.writeBoolean(true);
            Text.writeString(out, db);
        } else {
            out.writeBoolean(false);
        }
        Text.writeString(out, keyName);
    }

    private void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            db = Text.readString(in);
        }
        keyName = Text.readString(in);
    }

    public static EncryptKeyName read(DataInput in) throws IOException {
        EncryptKeyName encryptKeyName = new EncryptKeyName();
        encryptKeyName.readFields(in);
        return encryptKeyName;
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
}
