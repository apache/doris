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

package org.apache.doris.plugin;

import java.util.function.Supplier;

public class ProfileEvent extends Event {

    public String jobId = "";
    public String queryId = "";
    public String user = "";
    public String defaultDb = "";
    public String stmt = "";
    public String queryType = "";
    public String startTime = "";
    public String endTime = "";
    public String totalTime = "";
    public String queryState = "";
    public String traceId = "";
    /**
     * Use a supplier to lazy load profile.
     * Because profile content may be very large, so a profile plugin should filter profiles first as soon as possible.
     * A profile plugin should invoke profileSupplier.get() to load profile,
     * if and only if this plugin need to save or handle this profile.
     */
    public Supplier<String> profileSupplier;

    public static class ProfileEventBuilder {
        private ProfileEvent profileEvent = new ProfileEvent();

        public ProfileEventBuilder() {
        }

        public void reset() {
            profileEvent = new ProfileEvent();
        }

        public ProfileEventBuilder setJobId(String jobId) {
            profileEvent.jobId = jobId;
            return this;
        }

        public ProfileEventBuilder setQueryId(String queryId) {
            profileEvent.queryId = queryId;
            return this;
        }

        public ProfileEventBuilder setUser(String user) {
            profileEvent.user = user;
            return this;
        }

        public ProfileEventBuilder setDefaultDb(String defaultDb) {
            profileEvent.defaultDb = defaultDb;
            return this;
        }

        public ProfileEventBuilder setStmt(String stmt) {
            profileEvent.stmt = stmt;
            return this;
        }

        public ProfileEventBuilder setQueryType(String queryType) {
            profileEvent.queryType = queryType;
            return this;
        }

        public ProfileEventBuilder setStartTime(String startTime) {
            profileEvent.startTime = startTime;
            return this;
        }

        public ProfileEventBuilder setEndTime(String endTime) {
            profileEvent.endTime = endTime;
            return this;
        }

        public ProfileEventBuilder setTotalTime(String totalTimeMs) {
            profileEvent.totalTime = totalTimeMs;
            return this;
        }

        public ProfileEventBuilder setQueryState(String queryState) {
            profileEvent.queryState = queryState;
            return this;
        }

        public ProfileEventBuilder setTraceId(String traceId) {
            profileEvent.traceId = traceId;
            return this;
        }

        public ProfileEventBuilder setProfileSupplier(Supplier<String> supplier) {
            profileEvent.profileSupplier = supplier;
            return this;
        }

        public ProfileEvent build() {
            return profileEvent;
        }
    }
}
