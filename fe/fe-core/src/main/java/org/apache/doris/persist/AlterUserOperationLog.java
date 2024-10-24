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

package org.apache.doris.persist;

import org.apache.doris.analysis.AlterUserStmt;
import org.apache.doris.analysis.AlterUserStmt.OpType;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterUserOperationLog implements Writable {
    @SerializedName(value = "userIdent")
    private UserIdentity userIdent;
    @SerializedName(value = "password")
    private byte[] password;
    @SerializedName(value = "role")
    private String role;
    @SerializedName(value = "passwordOptions")
    private PasswordOptions passwordOptions;
    @SerializedName(value = "op")
    private AlterUserStmt.OpType op;

    @SerializedName(value = "comment")
    private String comment;

    public AlterUserOperationLog(OpType opType, UserIdentity userIdent, byte[] password,
            String role, PasswordOptions passwordOptions, String comment) {
        this.op = opType;
        this.userIdent = userIdent;
        this.password = password;
        this.role = role;
        this.passwordOptions = passwordOptions;
        this.comment = comment;
    }

    public OpType getOp() {
        return op;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public byte[] getPassword() {
        return password;
    }

    public String getRole() {
        return role;
    }

    public PasswordOptions getPasswordOptions() {
        return passwordOptions;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterUserOperationLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AlterUserOperationLog.class);
    }
}
