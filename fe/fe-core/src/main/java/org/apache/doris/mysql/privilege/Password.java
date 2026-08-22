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

import com.google.gson.annotations.SerializedName;

public class Password {
    @SerializedName(value = "pwd")
    private byte[] password;

    // MySQL-compatible dual password: the previous (retained) password, kept
    // valid for authentication until the next password change without
    // "RETAIN CURRENT PASSWORD", or an explicit "DISCARD OLD PASSWORD".
    // Nullable; absent in images written before this feature (GSON tolerant
    // in both directions).
    @SerializedName(value = "pwd2")
    private byte[] secondaryPassword;

    public Password() {
    }

    public Password(byte[] password) {
        this.password = password;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public byte[] getSecondaryPassword() {
        return secondaryPassword;
    }

    public void setSecondaryPassword(byte[] secondaryPassword) {
        this.secondaryPassword = secondaryPassword;
    }

    public boolean hasSecondaryPassword() {
        return secondaryPassword != null && secondaryPassword.length > 0;
    }
}
