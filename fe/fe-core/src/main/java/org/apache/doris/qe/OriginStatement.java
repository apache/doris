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

package org.apache.doris.qe;

import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;

/*
 * This class represents an origin statement
 * in multiple statements.
 */
public class OriginStatement {
    // the origin stmt from client. this may includes more than one statement.
    // eg: "select 1; select 2; select 3"
    @SerializedName(value = "originStmt")
    public final String originStmt;
    // the idx of the specified statement in "originStmt", start from 0.
    @SerializedName(value = "idx")
    public final int idx;

    public OriginStatement(String originStmt, int idx) {
        this.originStmt = originStmt;
        this.idx = idx;
    }

    public static OriginStatement read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, OriginStatement.class);
    }

    @Override
    public String toString() {
        return "OriginStatement{"
                + "originStmt='" + originStmt + '\''
                + ", idx=" + idx
                + '}';
    }
}
