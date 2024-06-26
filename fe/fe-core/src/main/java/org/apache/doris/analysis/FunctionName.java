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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FunctionName.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TFunctionName;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName {
    private static final Logger LOG = LogManager.getLogger(FunctionName.class);

    @SerializedName("db")
    private String db;
    @SerializedName("fn")
    private String fn;

    private FunctionName() {
    }

    public FunctionName(String db, String fn) {
        this.db = db;
        this.fn = fn.toLowerCase();
    }

    public FunctionName(String fn) {
        db = null;
        this.fn = fn.toLowerCase();
    }

    public FunctionName(TFunctionName thriftName) {
        db = thriftName.db_name.toLowerCase();
        fn = thriftName.function_name.toLowerCase();
    }

    // Same as FunctionName but for builtins and we'll leave the case
    // as is since we aren't matching by string.
    public static FunctionName createBuiltinName(String fn) {
        FunctionName name = new FunctionName(fn);
        name.fn = fn;
        return name;
    }

    public static FunctionName fromThrift(TFunctionName fnName) {
        return new FunctionName(fnName.getDbName(), fnName.getFunctionName());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionName)) {
            return false;
        }
        FunctionName o = (FunctionName) obj;
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
        return fn.equalsIgnoreCase(o.fn);
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public void setFn(String fn) {
        this.fn = fn;
    }

    public String getFunction() {
        return fn;
    }

    public boolean isFullyQualified() {
        return db != null;
    }

    @Override
    public String toString() {
        if (db == null) {
            return fn;
        }
        return db + "." + fn;
    }

    // used to analyze db element in function name, add cluster
    public String analyzeDb(Analyzer analyzer) throws AnalysisException {
        String db = this.db;
        if (db == null) {
            db = analyzer.getDefaultDb();
        }
        return db;
    }

    public void analyze(Analyzer analyzer, SetType type) throws AnalysisException {
        if (fn.length() == 0) {
            throw new AnalysisException("Function name can not be empty.");
        }
        for (int i = 0; i < fn.length(); ++i) {
            if (!isValidCharacter(fn.charAt(i))) {
                throw new AnalysisException("Function names must be all alphanumeric or underscore. "
                          + "Invalid name: " + fn);
            }
        }
        if (Character.isDigit(fn.charAt(0))) {
            throw new AnalysisException("Function cannot start with a digit: " + fn);
        }
        if (db == null) {
            db = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db) && type != SetType.GLOBAL) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
    }

    private boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    public TFunctionName toThrift() {
        TFunctionName name = new TFunctionName(fn);
        name.setDbName(db);
        name.setFunctionName(fn);
        return name;
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            db = Text.readString(in);
        }
        fn = Text.readString(in);
    }

    public static FunctionName read(DataInput in) throws IOException {
        FunctionName functionName = new FunctionName();
        functionName.readFields(in);
        return functionName;
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(db) + Objects.hashCode(fn);
    }
}
