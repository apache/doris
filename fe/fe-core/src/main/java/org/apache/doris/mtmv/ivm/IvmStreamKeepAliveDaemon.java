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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.TableStreamManager;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.EditLog.EditLogItem;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps the offsets of the idle partitions of IVM streams from expiring with changelog.ttl_seconds (F-2).
 *
 * <p>An offset moves only when a refresh consumes the partition, and a refresh consumes only the partitions with
 * new data, so the offset of a partition nobody writes stays at its last change. Once the partition has been idle
 * for longer than the TTL, the next write would make the refresh after it take the offset for expired and rebuild
 * the whole MV, though nothing the refresh needs is gone. Each round records a keep-alive TSO for such partitions
 * ({@link TableStreamManager#keepAliveIdlePartitions} stream by stream, and
 * {@link TableStreamManager#keepAliveIdleCloudPartitions} for all of them in cloud mode, where the offsets live in
 * MetaService), which the expiration check of a read takes in place of an older offset. Only IVM streams are kept
 * alive; a user stream is left to its consumer.
 */
public class IvmStreamKeepAliveDaemon extends MasterDaemon {
    // The daemon checks every second whether ivm_stream_keepalive_interval_second has passed since its last round,
    // so that a change of the interval applies at once: a daemon sleeping its own interval would only see it once
    // the old interval is over, and a sleep may not be cut short while an edit log is being written.
    private static final long CHECK_INTERVAL_MS = 1000L;

    // only read and written by the daemon thread
    private long lastRoundMs = 0;

    public IvmStreamKeepAliveDaemon() {
        super("ivm-stream-keepalive", CHECK_INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.enable_table_stream || !Config.enable_ivm_stream_keepalive) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastRoundMs < Config.ivm_stream_keepalive_interval_second * 1000L) {
            return;
        }
        lastRoundMs = now;
        keepAliveIdlePartitions();
    }

    @VisibleForTesting
    public void keepAliveIdlePartitions() {
        List<OlapTableStream> streams = new ArrayList<>();
        for (Database db : Env.getCurrentInternalCatalog().getDbs()) {
            for (Table table : db.getTables()) {
                if (!(table instanceof MTMV)) {
                    continue;
                }
                for (BaseTableStream stream : IvmUtil.getIvmStreams(db, (MTMV) table).values()) {
                    if (stream instanceof OlapTableStream) {
                        streams.add((OlapTableStream) stream);
                    }
                }
            }
        }
        TableStreamManager tableStreamManager = Env.getCurrentEnv().getTableStreamManager();
        List<EditLogItem> editLogItems = new ArrayList<>();
        if (Config.isCloudMode()) {
            editLogItems.addAll(tableStreamManager.keepAliveIdleCloudPartitions(streams));
        } else {
            for (OlapTableStream stream : streams) {
                tableStreamManager.keepAliveIdlePartitions(stream).ifPresent(editLogItems::add);
            }
        }
        editLogItems.forEach(EditLogItem::await);
    }
}
