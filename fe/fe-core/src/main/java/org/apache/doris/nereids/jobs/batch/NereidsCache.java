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

package org.apache.doris.nereids.jobs.batch;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.RewriteJob;
import org.apache.doris.nereids.rules.rewrite.logical.FetchPartitionCache;
import org.apache.doris.nereids.rules.rewrite.logical.FetchSqlCache;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Apply rules for cache.
 */
public class NereidsCache extends BatchRewriteJob {
    /**
     * CacheMode
     */
    public enum CacheMode {
        Sql,
        Partition
    }

    public static final List<RewriteJob> SQL_CACHE_JOBS = jobs(
            topic("SqlCache", bottomUp(
                new FetchSqlCache()
            ))
    );

    public static final List<RewriteJob> PARTITION_CACHE_JOBS = jobs(
            topic("PartitionCache", bottomUp(
                new FetchPartitionCache()
            ))
    );

    private CacheMode mode;

    public NereidsCache(CascadesContext cascadesContext, CacheMode mode) {
        super(cascadesContext);
        this.mode = mode;
    }

    @Override
    public List<RewriteJob> getJobs() {
        switch (mode) {
            case Sql:
                return SQL_CACHE_JOBS;
            case Partition:
                return PARTITION_CACHE_JOBS;
            default:
                return ImmutableList.of();
        }
    }
}
