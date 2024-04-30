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

import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;

import java.util.List;

public final class QueryStatisticsItem {

    private final String queryId;
    private final String user;
    private final String sql;
    private final String catalog;
    private final String db;
    private final String connId;
    private final long queryStartTime;
    private final List<FragmentInstanceInfo> fragmentInstanceInfos;
    // root query profile
    private final RuntimeProfile queryProfile;
    private final boolean isReportSucc;

    private QueryStatisticsItem(Builder builder) {
        this.queryId = builder.queryId;
        this.user = builder.user;
        this.sql = builder.sql;
        this.catalog = builder.catalog;
        this.db = builder.db;
        this.connId = builder.connId;
        this.queryStartTime = builder.queryStartTime;
        this.fragmentInstanceInfos = builder.fragmentInstanceInfos;
        this.queryProfile = builder.queryProfile;
        this.isReportSucc = builder.isReportSucc;
    }

    public String getDb() {
        return db;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getUser() {
        return user;
    }

    public String getSql() {
        return sql;
    }

    public String getConnId() {
        return connId;
    }

    public String getQueryExecTime() {
        final long currentTime = System.currentTimeMillis();
        if (queryStartTime <= 0) {
            return String.valueOf(-1);
        } else {
            return String.valueOf(currentTime - queryStartTime);
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public List<FragmentInstanceInfo> getFragmentInstanceInfos() {
        return fragmentInstanceInfos;
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public boolean getIsReportSucc() {
        return isReportSucc;
    }

    public static final class Builder {
        private String queryId;
        private String catalog;
        private String db;
        private String user;
        private String sql;
        private String connId;
        private long queryStartTime;
        private List<FragmentInstanceInfo> fragmentInstanceInfos;
        private RuntimeProfile queryProfile;
        private boolean isReportSucc;

        public Builder() {
            fragmentInstanceInfos = Lists.newArrayList();
        }

        public Builder queryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        public Builder db(String db) {
            this.db = db;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder connId(String connId) {
            this.connId = connId;
            return this;
        }

        public Builder queryStartTime(long queryStartTime) {
            this.queryStartTime = queryStartTime;
            return this;
        }

        public Builder fragmentInstanceInfos(List<FragmentInstanceInfo> infos) {
            fragmentInstanceInfos.addAll(infos);
            return this;
        }

        public Builder profile(RuntimeProfile profile) {
            this.queryProfile = profile;
            return this;
        }

        public Builder isReportSucc(boolean isReportSucc) {
            this.isReportSucc = isReportSucc;
            return this;
        }

        public QueryStatisticsItem build() {
            initDefaultValue(this);
            return new QueryStatisticsItem(this);
        }

        private void initDefaultValue(Builder builder) {
            if (queryId == null) {
                builder.queryId = "0";
            }

            if (db == null) {
                builder.db = "";
            }

            if (sql == null) {
                builder.sql = "";
            }

            if (user == null) {
                builder.user = "";
            }

            if (connId == null) {
                builder.connId = "";
            }

            if (queryProfile == null) {
                queryProfile = new RuntimeProfile("");
            }
        }
    }

    public static final class FragmentInstanceInfo {
        private final TUniqueId instanceId;
        private final TNetworkAddress address;
        private final String fragmentId;

        public FragmentInstanceInfo(Builder builder) {
            this.instanceId = builder.instanceId;
            this.address = builder.address;
            this.fragmentId = builder.fragmentId;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public String getFragmentId() {
            return this.fragmentId;
        }

        public static final class Builder {
            private TUniqueId instanceId;
            private TNetworkAddress address;
            private String fragmentId;

            public Builder instanceId(TUniqueId instanceId) {
                this.instanceId = instanceId;
                return this;
            }

            public Builder address(TNetworkAddress address) {
                this.address = address;
                return this;
            }

            public Builder fragmentId(String fragmentId) {
                this.fragmentId = fragmentId;
                return this;
            }

            public FragmentInstanceInfo build() {
                initDefaultValue(this);
                return new FragmentInstanceInfo(this);
            }

            private void initDefaultValue(Builder builder) {
                if (builder.instanceId == null) {
                    builder.instanceId = new TUniqueId(-1, -1);
                }

                if (builder.address == null) {
                    builder.address = new TNetworkAddress("null", -1);
                }

                if (builder.fragmentId == null) {
                    builder.fragmentId = "";
                }
            }
        }
    }
}
