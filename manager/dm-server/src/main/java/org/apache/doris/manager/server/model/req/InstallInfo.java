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
package org.apache.doris.manager.server.model.req;

public class InstallInfo {
    //host
    private String host;

    //role  be  fe
    private String role;

    //package url
    private String packageUrl;

    private boolean mkFeMetadir;

    private boolean mkBeStorageDir;

    private String installDir;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getPackageUrl() {
        return packageUrl;
    }

    public void setPackageUrl(String packageUrl) {
        this.packageUrl = packageUrl;
    }

    public boolean isMkFeMetadir() {
        return mkFeMetadir;
    }

    public void setMkFeMetadir(boolean mkFeMetadir) {
        this.mkFeMetadir = mkFeMetadir;
    }

    public String getInstallDir() {
        return installDir;
    }

    public void setInstallDir(String installDir) {
        this.installDir = installDir;
    }

    public boolean isMkBeStorageDir() {
        return mkBeStorageDir;
    }

    public void setMkBeStorageDir(boolean mkBeStorageDir) {
        this.mkBeStorageDir = mkBeStorageDir;
    }
}
