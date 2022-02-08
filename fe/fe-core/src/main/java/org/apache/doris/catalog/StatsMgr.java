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

package org.apache.doris.catalog;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StatsMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(StatsMgr.class);
    public StatsMgr() {
    }

    public static class Stats implements Writable{
        @SerializedName("fe")
        String fe;

        @SerializedName("queryNum")
        long queryNum;

        public Stats(String fe, long queryNum) {
            this.fe = fe;
            this.queryNum = queryNum;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }

        public static Stats read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, Stats.class);
        }
    }

    @SerializedName("feToStats")
    private final Map<String, Stats> feToStats = Maps.newConcurrentMap();
    private final AtomicLong localQueryNum = new AtomicLong(0);
    private long totalQueryNum = 0;

    public boolean checkQueryAccess() {
        totalQueryNum = 0;
        for (Map.Entry<String, Stats> entry: feToStats.entrySet()) {
            totalQueryNum += entry.getValue().queryNum;
        }
        return totalQueryNum < Config.max_running_query_num;
    }

    public long getTotalQueryNum() {
        return totalQueryNum;
    }

    public long increaseQueryNum() {
        return this.localQueryNum.getAndIncrement();
    }

    public long getAndResetQueryNum() {
        return this.localQueryNum.getAndSet(0);
    }

    public void setStats(Stats stats) {
        feToStats.put(stats.fe, stats);
        Catalog.getCurrentCatalog().getEditLog().logSetStats(stats);
    }

    public void replaySetStats(Stats stats) {
        feToStats.put(stats.fe, stats);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static StatsMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StatsMgr.class);
    }
}
