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

import org.apache.doris.alter.AlterUserOpType;
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
    private AlterUserOpType op;

    @SerializedName(value = "comment")
    private String comment;

    // MySQL-compatible "ALTER USER ... DISCARD OLD PASSWORD" marker. Such an
    // entry is journaled with op = SET_PASSWORD_POLICY and every option UNSET,
    // so an FE binary WITHOUT this feature (which ignores this field) replays
    // it as a password-policy update that changes nothing, while a binary with
    // the feature drops the retained secondary password. Absent in journals
    // written before this feature (GSON default: false). See
    // Auth.discardOldPasswordInternal.
    @SerializedName(value = "discardOldPasswd")
    private boolean discardOldPassword;

    public AlterUserOperationLog(AlterUserOpType opType, UserIdentity userIdent, byte[] password,
                                 String role, PasswordOptions passwordOptions, String comment) {
        this.op = opType;
        this.userIdent = userIdent;
        this.password = password;
        this.role = role;
        this.passwordOptions = passwordOptions;
        this.comment = comment;
    }

    /**
     * The journal entry for "ALTER USER ... DISCARD OLD PASSWORD". Its carrier
     * op is SET_PASSWORD_POLICY with {@link PasswordOptions#UNSET_OPTION}: on
     * a pre-feature binary that replays as a no-op (every UNSET branch of the
     * policy update returns early and no password is journaled), whereas an
     * unknown AlterUserOpType name would deserialize as null and fail replay,
     * and an OP_SET_PASSWORD carrier would append the primary to the password
     * history and refresh the password creation time.
     */
    public static AlterUserOperationLog discardOldPassword(UserIdentity userIdent) {
        AlterUserOperationLog log = new AlterUserOperationLog(AlterUserOpType.SET_PASSWORD_POLICY, userIdent,
                null, null, PasswordOptions.UNSET_OPTION, null);
        log.discardOldPassword = true;
        return log;
    }

    public AlterUserOpType getOp() {
        return op;
    }

    public boolean isDiscardOldPassword() {
        return discardOldPassword;
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
