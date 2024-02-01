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

public class RedirectStatus {
    private boolean isForwardToMaster;
    private boolean needToWaitJournalSync;

    public RedirectStatus() {
        isForwardToMaster = true;
        needToWaitJournalSync  = true;
    }

    public RedirectStatus(boolean isForwardToMaster, boolean needToWaitJournalSync) {
        this.isForwardToMaster = isForwardToMaster;
        this.needToWaitJournalSync = needToWaitJournalSync;
    }

    public boolean isForwardToMaster() {
        return isForwardToMaster;
    }

    public void setForwardToMaster(boolean isForwardToMaster) {
        this.isForwardToMaster = isForwardToMaster;
    }

    public boolean isNeedToWaitJournalSync() {
        return needToWaitJournalSync;
    }

    public void setNeedToWaitJournalSync(boolean needToWaitJournalSync) {
        this.needToWaitJournalSync = needToWaitJournalSync;
    }

    public static RedirectStatus FORWARD_NO_SYNC = new RedirectStatus(true, false);
    public static RedirectStatus FORWARD_WITH_SYNC = new RedirectStatus(true, true);
    public static RedirectStatus NO_FORWARD =   new RedirectStatus(false, false);
}
