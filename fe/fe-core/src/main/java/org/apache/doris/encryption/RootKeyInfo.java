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

package org.apache.doris.encryption;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

public class RootKeyInfo {
    public enum RootKeyType {
        LOCAL("local"),
        AWS_KMS("aws_kms");

        public static RootKeyType tryFrom(String name) {
            Objects.requireNonNull(name);
            if (LOCAL.name.equalsIgnoreCase(name)) {
                return LOCAL;
            }
            if (AWS_KMS.name.equalsIgnoreCase(name)) {
                return AWS_KMS;
            }
            throw new IllegalArgumentException("invalid name" + name);
        }

        RootKeyType(String name) {
            this.name = name;
        }

        String name;
    }

    public RootKeyInfo() {}

    public RootKeyInfo(RootKeyInfo info) {
        this.type = info.type;
        this.algorithm = info.algorithm;
        this.region = info.region;
        this.endpoint = info.endpoint;
        this.cmkId = info.cmkId;
        this.ak = info.ak;
        this.sk = info.sk;
        this.password = info.password;
    }

    @SerializedName(value = "type")
    public RootKeyType type;

    @SerializedName(value = "algorithm")
    public EncryptionKey.Algorithm algorithm;

    @SerializedName(value = "region")
    public String region;

    @SerializedName(value = "endpoint")
    public String endpoint;

    @SerializedName(value = "cmkId")
    public String cmkId;

    @SerializedName(value = "ak")
    public String ak;

    @SerializedName(value = "sk")
    public String sk;

    @SerializedName(value = "password")
    public String password;
}

