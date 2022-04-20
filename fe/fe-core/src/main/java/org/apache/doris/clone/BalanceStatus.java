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

package org.apache.doris.clone;

import com.google.common.collect.Lists;

import java.util.List;

public class BalanceStatus {
    public enum ErrCode {
        OK,
        META_NOT_FOUND,
        STATE_NOT_NORMAL,
        COMMON_ERROR
    }

    private ErrCode errCode;
    private List<String> errMsgs = Lists.newArrayList();

    public static final BalanceStatus OK = new BalanceStatus(ErrCode.OK, "");

    public BalanceStatus(ErrCode errCode) {
        this.errCode = errCode;
    }

    public BalanceStatus(ErrCode errCode, String errMsg) {
        this.errCode = errCode;
        this.errMsgs.add(errMsg);
    }

    public ErrCode getErrCode() {
        return errCode;
    }

    public List<String> getErrMsgs() {
        return errMsgs;
    }

    public void addErrMsgs(List<String> errMsgs) {
        this.errMsgs.addAll(errMsgs);
    }

    public void addErrMsg(String errMsg) {
        this.errMsgs.add(errMsg);
    }

    public boolean ok() {
        return errCode == ErrCode.OK;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(errCode.name());
        if (!ok()) {
            sb.append(", msg: ").append(errMsgs);
        }
        sb.append("]");
        return sb.toString();
    }
}
