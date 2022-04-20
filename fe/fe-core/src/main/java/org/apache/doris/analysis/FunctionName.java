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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TFunctionName;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName implements Writable {
    private static final Logger LOG = LogManager.getLogger(FunctionName.class);

    private String db_;
    private String fn_;

    private FunctionName() {
    }

    public FunctionName(String db, String fn) {
        db_ = db;
        fn_ = fn.toLowerCase();
        if (db_ != null) {
            db_ = db_.toLowerCase();
        }
    }

    public FunctionName(String fn) {
        db_ = null;
        fn_ = fn.toLowerCase();
    }

    public FunctionName(TFunctionName thriftName) {
        db_ = thriftName.db_name.toLowerCase();
        fn_ = thriftName.function_name.toLowerCase();
    }

    // Same as FunctionName but for builtins and we'll leave the case
    // as is since we aren't matching by string.
    public static FunctionName createBuiltinName(String fn) {
        FunctionName name = new FunctionName(fn);
        name.fn_ = fn;
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
        if ((db_ == null || o.db_ == null) && (db_ != o.db_)) {
            if (db_ == null && o.db_ != null) {
                return false;
            }
            if (db_ != null && o.db_ == null) {
                return false;
            }
            if (!db_.equalsIgnoreCase(o.db_)) {
                return false;
            }
        }
        return fn_.equalsIgnoreCase(o.fn_);
    }

    public String getDb() {
        return db_;
    }

    public void setDb(String db) {
        db_ = db;
    }

    public String getFunction() {
        return fn_;
    }

    public boolean isFullyQualified() {
        return db_ != null;
    }

    @Override
    public String toString() {
        if (db_ == null) {
            return fn_;
        }
        return db_ + "." + fn_;
    }

    // used to analyze db element in function name, add cluster
    public String analyzeDb(Analyzer analyzer) throws AnalysisException {
        String db = db_;
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
        if (fn_.length() == 0) {
            throw new AnalysisException("Function name can not be empty.");
        }
        for (int i = 0; i < fn_.length(); ++i) {
            if (!isValidCharacter(fn_.charAt(i))) {
                throw new AnalysisException(
                  "Function names must be all alphanumeric or underscore. " +
                    "Invalid name: " + fn_);
            }
        }
        if (Character.isDigit(fn_.charAt(0))) {
            throw new AnalysisException("Function cannot start with a digit: " + fn_);
        }
        if (db_ == null) {
            db_ = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(db_)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
            }
            db_ = ClusterNamespace.getFullName(analyzer.getClusterName(), db_);
        }

        // If the function name is not fully qualified, it must not be the same as a builtin
//        if (!isFullyQualified() && OpcodeRegistry.instance().getFunctionOperator(
//          getFunction()) != FunctionOperator.INVALID_OPERATOR) {
//            throw new AnalysisException(
//              "Function cannot have the same name as a builtin: " + getFunction());
//        }
    }

    private boolean isValidCharacter(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    public TFunctionName toThrift() {
        TFunctionName name = new TFunctionName(fn_);
        name.setDbName(db_);
        name.setFunctionName(fn_);
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (db_ != null) {
            out.writeBoolean(true);
            Text.writeString(out, db_);
        } else {
            out.writeBoolean(false);
        }
        Text.writeString(out, fn_);
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            db_ = Text.readString(in);
        }
        fn_ = Text.readString(in);
    }

    public static FunctionName read(DataInput in) throws IOException{
        FunctionName functionName = new FunctionName();
        functionName.readFields(in);
        return functionName;
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(db_) + Objects.hashCode(fn_);
    }
}
