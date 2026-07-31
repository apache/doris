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

package org.apache.doris.catalog;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName {
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

    // Same as FunctionName but for builtins and we'll leave the case
    // as is since we aren't matching by string.
    public static FunctionName createBuiltinName(String fn) {
        FunctionName name = new FunctionName(fn);
        name.fn = fn;
        return name;
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

    @Override
    public String toString() {
        if (db == null) {
            return fn;
        }
        return db + "." + fn;
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(db.toLowerCase()) + Objects.hashCode(fn.toLowerCase());
    }

    @Override
    public FunctionName clone() {
        return new FunctionName(db, fn);
    }
}
