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
package org.pentaho.di.trans.steps.dorisstreamloader.load;

import javax.annotation.Nullable;
import java.util.Optional;

public class DorisJdbcConnectionOptions {

    private final String url;
    private final String driverName;
    private final String cjDriverName;
    @Nullable
    private final String username;
    @Nullable
    private final String password;

    public DorisJdbcConnectionOptions(String url, String username, String password) {
        this.url = url;
        this.driverName = "com.mysql.jdbc.Driver";
        this.cjDriverName = "com.mysql.cj.jdbc.Driver";
        this.username = username;
        this.password = password;
    }

    public String getDbURL() {
        return url;
    }

    public String getCjDriverName() {
        return cjDriverName;
    }

    public String getDriverName() {
        return driverName;
    }

    public Optional<String> getUsername() {
        return Optional.ofNullable(username);
    }

    public Optional<String> getPassword() {
        return Optional.ofNullable(password);
    }
}
