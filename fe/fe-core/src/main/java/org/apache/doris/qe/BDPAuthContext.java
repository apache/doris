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

import org.apache.doris.thrift.TBDPAuthContext;

import com.alibaba.ttl.TransmittableThreadLocal;

public class BDPAuthContext {
    protected static TransmittableThreadLocal<BDPAuthContext> threadLocalInfo = new TransmittableThreadLocal<>();
    private String erp = null;
    private String source = null;
    private String hadoopUserName = null;
    private String userToken = null;
    private volatile boolean erpChanged = false;

    public BDPAuthContext() {
    }

    public BDPAuthContext(TBDPAuthContext bdpAuthContext) {
        this(bdpAuthContext.erp, bdpAuthContext.source, bdpAuthContext.hadoopUserName, bdpAuthContext.userToken);
    }

    public BDPAuthContext(String erp, String source, String hadoopUserName, String userToken) {
        this.erp = erp;
        this.source = source;
        this.hadoopUserName = hadoopUserName;
        this.userToken = userToken;
    }

    public void setErpChanged(boolean erpChanged) {
        this.erpChanged = erpChanged;
    }

    public boolean isErpChanged() {
        return erpChanged;
    }

    public void setErp(String erp) {
        this.erp = erp;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setHadoopUserName(String hadoopUserName) {
        this.hadoopUserName = hadoopUserName;
    }

    public void setUserToken(String userToken) {
        this.userToken = userToken;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public static BDPAuthContext get() {
        return threadLocalInfo.get();
    }

    public static void clear() {
        threadLocalInfo.remove();
    }

    public String getErp() {
        return erp;
    }

    public String getSource() {
        return source;
    }

    public String getHadoopUserName() {
        return hadoopUserName;
    }

    public String getUserToken() {
        return userToken;
    }

    public String toString() {
        return String.format("bdp_auth_context[erp: %s, source: %s, hadoop_user_name: %s]",
            erp, source, hadoopUserName);
    }
}
