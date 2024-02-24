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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Password policy for a specific user.
 * Current support:
 *  PASSWORD EXPIRE
 *  PASSWORD HISTORY
 *  FAILED_LOGIN_ATTEMPTS
 *  PASSWORD_LOCK_TIME
 */
public class PasswordPolicy implements Writable {
    private static final Logger LOG = LogManager.getLogger(PasswordPolicy.class);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final String EXPIRATION_SECONDS = "password_policy.expiration_seconds";
    private static final String PASSWORD_CREATION_TIME = "password_policy.password_creation_time";
    private static final String HISTORY_NUM = "password_policy.history_num";
    private static final String HISTORY_PASSWORDS = "password_policy.history_passwords";
    private static final String NUM_FAILED_LOGIN = "password_policy.num_failed_login";
    private static final String PASSWORD_LOCK_SECONDS = "password_policy.password_lock_seconds";
    private static final String FAILED_LOGIN_COUNTER = "password_policy.failed_login_counter";
    private static final String LOCK_TIME = "password_policy.lock_time";

    @SerializedName(value = "expirePolicy")
    private ExpirePolicy expirePolicy = new ExpirePolicy();
    @SerializedName(value = "historyPolicy")
    private HistoryPolicy historyPolicy = new HistoryPolicy();
    @SerializedName(value = "failedLoginPolicy")
    private FailedLoginPolicy failedLoginPolicy = new FailedLoginPolicy();

    public PasswordPolicy() {
    }

    public static PasswordPolicy createDefault() {
        return new PasswordPolicy();
    }

    public void checkAccountLockedAndPasswordExpiration(UserIdentity curUser) throws AuthenticationException {
        lock.readLock().lock();
        try {
            if (expirePolicy.isExpire()) {
                throw new AuthenticationException(ErrorCode.ERR_MUST_CHANGE_PASSWORD_LOGIN);
            }
            if (failedLoginPolicy.isLocked()) {
                throw new AuthenticationException(
                        ErrorCode.ERR_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK,
                        curUser.getQualifiedUser(), curUser.getHost(),
                        failedLoginPolicy.passwordLockSeconds,
                        failedLoginPolicy.leftSeconds(),
                        failedLoginPolicy.failedLoginCounter);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean onFailedLogin() {
        lock.writeLock().lock();
        try {
            return failedLoginPolicy.onFailedLogin();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean checkPasswordHistory(byte[] password) {
        lock.readLock().lock();
        try {
            return historyPolicy.isPasswordAllowed(password);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void update(byte[] password, PasswordOptions passwordOptions) {
        lock.writeLock().lock();
        try {
            expirePolicy.update(passwordOptions.getExpirePolicySecond());
            historyPolicy.update(password, passwordOptions.getHistoryPolicy());
            failedLoginPolicy.updateNumFailedLogin(passwordOptions.getLoginAttempts());
            failedLoginPolicy.updatePasswordLockSeconds(passwordOptions.getPasswordLockSecond());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updatePassword(byte[] password) {
        lock.writeLock().lock();
        try {
            historyPolicy.addPassword(password);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public ExpirePolicy getExpirePolicy() {
        return expirePolicy;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static PasswordPolicy read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), PasswordPolicy.class);
    }

    public List<List<String>> getInfo() {
        lock.readLock().lock();
        try {
            List<List<String>> rows = Lists.newArrayList();
            expirePolicy.getInfo(rows);
            historyPolicy.getInfo(rows);
            failedLoginPolicy.getInfo(rows);
            return rows;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void unlockAccount() {
        lock.writeLock().lock();
        try {
            failedLoginPolicy.unlock();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Password expire policy.
     * If a password is expired, user can no longer login with this password
     */
    public static class ExpirePolicy implements Writable {
        // -1: default, will use session variable: default_password_lifetime
        // 0: never
        // > 0: seconds
        public static final int DEFAULT = -1;
        public static final int NEVER = 0;

        @SerializedName(value = "expirationSecond")
        public long expirationSecond = NEVER;
        @SerializedName(value = "passwordCreateTime")
        public long passwordCreateTime = 0;

        public boolean isExpire() {
            return leftSeconds() <= 0;
        }

        public long leftSeconds() {
            long tmp = expirationSecond;
            if (tmp == -1) {
                tmp = GlobalVariable.defaultPasswordLifetime * 86400;
            }
            if (tmp == 0) {
                return Long.MAX_VALUE;
            }
            return tmp - (System.currentTimeMillis() - passwordCreateTime) / 1000;
        }

        public void update(long expirationSecond) {
            if (expirationSecond == PasswordOptions.UNSET) {
                return;
            }
            this.expirationSecond = expirationSecond;
            this.passwordCreateTime = System.currentTimeMillis();
        }

        public void setPasswordCreateTime() {
            this.passwordCreateTime = System.currentTimeMillis();
        }

        private String expirationSecondsToString() {
            if (expirationSecond == -1) {
                return "DEFAULT";
            } else if (expirationSecond == 0) {
                return "NEVER";
            } else {
                return String.valueOf(expirationSecond);
            }
        }

        public void getInfo(List<List<String>> rows) {
            List<String> row1 = Lists.newArrayList();
            row1.add(EXPIRATION_SECONDS);
            row1.add(expirationSecondsToString());
            List<String> row2 = Lists.newArrayList();
            row2.add(PASSWORD_CREATION_TIME);
            row2.add(passwordCreateTime == 0 ? "" : TimeUtils.longToTimeString(passwordCreateTime));
            rows.add(row1);
            rows.add(row2);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static ExpirePolicy read(DataInput in) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(in), ExpirePolicy.class);
        }
    }

    /**
     * Password history num.
     * The password saved in this policy can not be reused.
     */
    public static class HistoryPolicy implements Writable {
        public static final int MAX_HISTORY_SIZE = 10;
        public static final int DEFAULT = -1;
        public static final int NO_RESTRICTION = 0;

        @SerializedName(value = "historyPasswords")
        public Queue<byte[]> historyPasswords = Queues.newArrayDeque();
        // -1: default, will use session variable: password_history
        // 0: no reuse restriction
        // > 0: number of history password that can not be reused
        @SerializedName(value = "historyNum")
        public int historyNum = NO_RESTRICTION;

        public boolean isPasswordAllowed(byte[] password) {
            if (historyNum == NO_RESTRICTION) {
                return true;
            }
            Iterator<byte[]> iter = historyPasswords.iterator();
            int tmp = historyNum;
            if (tmp == DEFAULT) {
                tmp = GlobalVariable.passwordHistory;
            }
            // if number of password saved in historyPasswords is more than historyNum,
            // we need to move forward to pass those history password that do not need to check.
            int forward = historyPasswords.size() <= tmp ? 0 : (historyPasswords.size() - tmp);
            while (forward-- > 0) {
                iter.next();
            }
            while (iter.hasNext()) {
                byte[] history = iter.next();
                if (Arrays.equals(history, password)) {
                    return false;
                }
            }
            return true;
        }

        public void addPassword(byte[] password) {
            if (historyPasswords.size() == MAX_HISTORY_SIZE) {
                historyPasswords.poll();
            }
            historyPasswords.add(password);
        }

        public void update(byte[] password, int historyNum) {
            if (historyNum != PasswordOptions.UNSET) {
                this.historyNum = historyNum;
                this.historyPasswords.clear();
            }
            if (password != null) {
                this.historyPasswords.add(password);
            }
        }

        private String historyNumToString() {
            switch (historyNum) {
                case -1:
                    return "DEFAULT";
                case 0:
                    return "NO_RESTRICTION";
                default:
                    return String.valueOf(historyNum);
            }
        }

        public void getInfo(List<List<String>> rows) {
            List<String> row1 = Lists.newArrayList();
            row1.add(HISTORY_NUM);
            row1.add(historyNumToString());
            List<String> row2 = Lists.newArrayList();
            row2.add(HISTORY_PASSWORDS);
            List<String> hexPasswords = Lists.newArrayList();
            Iterator<byte[]> iter = historyPasswords.iterator();
            // if number of password saved in historyPasswords is more than historyNum,
            // we need to move forward to pass those history password that do not need to check.
            int tmp = historyNum;
            if (tmp == DEFAULT) {
                tmp = GlobalVariable.passwordHistory;
            }
            int forward = historyPasswords.size() <= tmp ? 0 : (historyPasswords.size() - tmp);
            while (forward-- > 0) {
                iter.next();
            }
            while (iter.hasNext()) {
                byte[] history = iter.next();
                String hex = Util.bytesToHex(history);
                hexPasswords.add("*" + hex.substring(0, Math.min(3, hex.length())) + "...");
            }
            row2.add(Joiner.on(",").join(hexPasswords));
            rows.add(row1);
            rows.add(row2);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static HistoryPolicy read(DataInput in) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(in), HistoryPolicy.class);
        }
    }

    /**
     * If a user is failed to login for a certain time,
     * the account will be locked for a certain period.
     */
    public static class FailedLoginPolicy implements Writable {
        public static final int DISABLED = 0;
        public static final int UNBOUNDED = -1;
        public static final int LOCK_ACCOUNT = -1;
        public static final int UNLOCK_ACCOUNT = 1;
        // 0: disabled
        // > 0: num of failed login
        @SerializedName(value = "numFailedLogin")
        public int numFailedLogin = DISABLED;
        // -1: unbounded
        // 0: disabled
        // > 0: lock time (seconds)
        @SerializedName(value = "passwordLockSeconds")
        public long passwordLockSeconds = DISABLED;
        // num of current failed login
        // This field will not persist, so that each FE is independent.
        // And after a FE restart, it will be reset to 0.
        // Use atomic because it will be visited and updated by multi threads.
        public AtomicLong failedLoginCounter = new AtomicLong(0);
        // time when the account being locked.
        // Same as failedLoginCounter, not persist
        public AtomicLong lockTime = new AtomicLong(0);

        // Return true if the account is being locked.
        // Return false if nothing happen.
        public boolean onFailedLogin() {
            if (numFailedLogin == DISABLED || passwordLockSeconds == DISABLED) {
                // This policy is disabled, nothing happen
                return false;
            }
            if (failedLoginCounter.get() >= numFailedLogin) {
                return true;
            }
            if (failedLoginCounter.incrementAndGet() >= numFailedLogin) {
                lockTime.set(System.currentTimeMillis());
                return true;
            }
            return false;
        }

        public boolean isLocked() {
            return leftSeconds() > 0;
        }

        public long leftSeconds() {
            if (numFailedLogin == DISABLED || passwordLockSeconds == DISABLED || lockTime.get() == 0) {
                // This policy is disabled or not locked, return
                return 0;
            }
            if (lockTime.get() > 0 && passwordLockSeconds == UNBOUNDED) {
                // unbounded lock
                // Returns 9999 seconds every time instead of 9999 seconds countdown
                return 9999;
            }
            return Math.max(0, passwordLockSeconds - ((System.currentTimeMillis() - lockTime.get()) / 1000));
        }

        public void updateNumFailedLogin(int numFailedLogin) {
            if (numFailedLogin == PasswordOptions.UNSET) {
                return;
            }
            this.numFailedLogin = numFailedLogin;
            unlock();
        }

        public void updatePasswordLockSeconds(long passwordLockSeconds) {
            if (passwordLockSeconds == PasswordOptions.UNSET) {
                return;
            }
            this.passwordLockSeconds = passwordLockSeconds;
            unlock();
        }

        public void unlock() {
            this.failedLoginCounter.set(0);
            this.lockTime.set(0);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static FailedLoginPolicy read(DataInput in) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(in), FailedLoginPolicy.class);
        }

        private String passwordLockSecondsToString() {
            if (passwordLockSeconds == -1) {
                return "UNBOUNDED";
            } else if (passwordLockSeconds == 0) {
                return "DISABLED";
            } else {
                return String.valueOf(passwordLockSeconds);
            }
        }

        private String numFailedLoginToString() {
            switch (numFailedLogin) {
                case 0:
                    return "DISABLED";
                default:
                    return String.valueOf(numFailedLogin);
            }
        }

        public void getInfo(List<List<String>> rows) {
            List<String> row1 = Lists.newArrayList();
            row1.add(NUM_FAILED_LOGIN);
            row1.add(numFailedLoginToString());
            List<String> row2 = Lists.newArrayList();
            row2.add(PASSWORD_LOCK_SECONDS);
            row2.add(passwordLockSecondsToString());
            List<String> row3 = Lists.newArrayList();
            row3.add(FAILED_LOGIN_COUNTER);
            row3.add(String.valueOf(failedLoginCounter.get()));
            List<String> row4 = Lists.newArrayList();
            row4.add(LOCK_TIME);
            row4.add(lockTime.get() == 0 ? "" : TimeUtils.longToTimeString(lockTime.get()));
            rows.add(row1);
            rows.add(row2);
            rows.add(row3);
            rows.add(row4);
        }
    }
}
