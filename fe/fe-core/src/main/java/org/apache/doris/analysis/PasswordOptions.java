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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PasswordPolicy.HistoryPolicy;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PasswordOptions implements Writable {

    public static final int UNSET = -2;
    public static final PasswordOptions UNSET_OPTION = new PasswordOptions(UNSET, UNSET, UNSET, UNSET, UNSET, UNSET);

    // -2: not set
    // -1: default, use default_password_lifetime
    // 0: disabled
    // > 0: expire day
    @SerializedName(value = "expirePolicySecond")
    private long expirePolicySecond;
    // -2: not set
    // -1: default, use password_history
    // 0: disabled
    // > 0: num of history passwords
    @SerializedName(value = "historyPolicy")
    private int historyPolicy;
    // -2: not set
    @SerializedName(value = "reusePolicy")
    private int reusePolicy;
    // -2: not set
    // 0: disable
    // > 0:
    @SerializedName(value = "loginAttempts")
    private int loginAttempts;
    // -2: not set
    // -1: unbounded
    // 0: disabled
    // > 0: lock days
    @SerializedName(value = "passwordLockSecond")
    private long passwordLockSecond;

    // -2: not set
    // -1: lock the account
    // 1: unlock the account
    @SerializedName(value = "accountUnlocked")
    private int accountUnlocked;

    public PasswordOptions(long expirePolicySecond, int historyPolicy, int reusePolicy,
            int loginAttempts, long passwordLockSecond, int accountUnlocked) {
        this.expirePolicySecond = expirePolicySecond;
        this.historyPolicy = historyPolicy;
        this.reusePolicy = reusePolicy;
        this.loginAttempts = loginAttempts;
        this.passwordLockSecond = passwordLockSecond;
        this.accountUnlocked = accountUnlocked;
    }

    public long getExpirePolicySecond() {
        return expirePolicySecond;
    }

    public int getHistoryPolicy() {
        return historyPolicy;
    }

    public int getReusePolicy() {
        return reusePolicy;
    }

    public int getLoginAttempts() {
        return loginAttempts;
    }

    public long getPasswordLockSecond() {
        return passwordLockSecond;
    }

    public int getAccountUnlocked() {
        return accountUnlocked;
    }

    public void analyze() throws AnalysisException {
        if (expirePolicySecond < -2L) {
            throw new AnalysisException("The password expire time must be DAFAULT or >= 0");
        }
        if (historyPolicy < -2 || historyPolicy > HistoryPolicy.MAX_HISTORY_SIZE) {
            throw new AnalysisException(
                    "The password history number must be DEFAULT or between 0 and " + HistoryPolicy.MAX_HISTORY_SIZE);
        }
        if (reusePolicy != -2) {
            throw new AnalysisException("Not support setting password reuse policy now");
        }
        if (loginAttempts < -2 || loginAttempts == -1 || loginAttempts > 32767) {
            throw new AnalysisException("The failed login attempts must between 0 and 32767");
        }
        if (passwordLockSecond < -2L || passwordLockSecond > 32767L * 86400) {
            throw new AnalysisException("The account lock time after consecutive failure login"
                    + " must be >= 0, or UNBOUNDED");
        }
        if (accountUnlocked != -2 && accountUnlocked != -1 && accountUnlocked != 1) {
            throw new AnalysisException("Invalid account lock/unlock option. Should be ACCOUNT_LOCK or ACCOUNT_UNLOCK");
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (expirePolicySecond >= 0) {
            sb.append(" PASSWORD EXPIRE ")
                    .append(expirePolicySecond == 0 ? "NEVER" : "INTERVAL " + expirePolicySecond + " SECOND");
        }
        if (historyPolicy > 0) {
            sb.append(" PASSWORD HISTORY").append(historyPolicy);
        }
        if (loginAttempts > 0) {
            sb.append(" FAILED_LOGIN_ATTEMPTS ").append(loginAttempts);
        }
        if (passwordLockSecond > 0) {
            sb.append(" PASSWORD_LOCK_TIME ").append(passwordLockSecond).append(" SECOND");
        }
        if (accountUnlocked != -2) {
            sb.append(accountUnlocked == -1 ? " ACCOUNT_LOCK" : " ACCOUNT_UNLOCK");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static PasswordOptions read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PasswordOptions.class);
    }
}
