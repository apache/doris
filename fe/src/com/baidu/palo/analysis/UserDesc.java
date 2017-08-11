// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

// Description of user in SQL statement
public class UserDesc {
    private String user;
    private String password;
    private boolean isPlain;

    public UserDesc(String user) {
        this(user, "", false);
    }

    public UserDesc(String user, String password, boolean isPlain) {
        this.user = user;
        this.password = password;
        this.isPlain = isPlain;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public boolean isPlain() {
        return isPlain;
    }

    public void setPlain(boolean isPlain) {
        this.isPlain = isPlain;
    }
}
