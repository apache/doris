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

package org.apache.doris.task;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletMetaInfo;
import org.apache.doris.thrift.TTabletMetaType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUpdateTabletMetaInfoReq;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

public class UpdateTabletMetaInfoTask extends AgentTask {

    private static final Logger LOG = LogManager.getLogger(UpdateTabletMetaInfoTask.class);

    // used for synchronous process
    private MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch;

    private Set<Pair<Long, Integer>> tableIdWithSchemaHash;
    private boolean isInMemory;
    private TTabletMetaType metaType;

    // <tablet id, tablet schema hash, tablet in memory>
    private List<Triple<Long, Integer, Boolean>> tabletToInMemory;

    public UpdateTabletMetaInfoTask(long backendId, Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    TTabletMetaType metaType) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO,
                -1L, -1L, -1L, -1L, -1L, tableIdWithSchemaHash.hashCode());
        this.tableIdWithSchemaHash = tableIdWithSchemaHash;
        this.metaType = metaType;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    boolean isInMemory,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch) {
        this(backendId, tableIdWithSchemaHash, TTabletMetaType.INMEMORY);
        this.isInMemory = isInMemory;
        this.latch = latch;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    List<Triple<Long, Integer, Boolean>> tabletToInMemory) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO,
                -1L, -1L, -1L, -1L, -1L, tabletToInMemory.hashCode());
        this.metaType = TTabletMetaType.INMEMORY;
        this.tabletToInMemory = tabletToInMemory;
    }

    public void countDownLatch(long backendId, Set<Pair<Long, Integer>> tablets) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tablets)) {
                LOG.debug("UpdateTabletMetaInfoTask current latch count: {}, backend: {}, tablets:{}",
                        latch.getCount(), backendId, tablets);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("UpdateTabletMetaInfoTask count down to zero. error msg: {}", errMsg);
        }
    }

    public Set<Pair<Long, Integer>> getTablets() {
        return tableIdWithSchemaHash;
    }

    public TUpdateTabletMetaInfoReq toThrift() {
        TUpdateTabletMetaInfoReq updateTabletMetaInfoReq = new TUpdateTabletMetaInfoReq();
        List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
        switch (metaType) {
            case PARTITIONID: {
                int tabletEntryNum = 0;
                for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                    // add at most 10000 tablet meta during one sync to avoid too large task
                    if (tabletEntryNum > 10000) {
                        break;
                    }
                    TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                    metaInfo.setTabletId(pair.first);
                    metaInfo.setSchemaHash(pair.second);
                    TabletMeta tabletMeta = Catalog.getCurrentCatalog().getTabletInvertedIndex().getTabletMeta(pair.first);
                    if (tabletMeta == null) {
                        LOG.warn("could not find tablet [{}] in meta ignore it", pair.second);
                        continue;
                    }
                    metaInfo.setPartitionId(tabletMeta.getPartitionId());
                    metaInfo.setMetaType(metaType);
                    metaInfos.add(metaInfo);
                    ++tabletEntryNum;
                }
                break;
            }
            case INMEMORY: {
                if (latch != null) {
                    // for schema change
                    for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTabletId(pair.first);
                        metaInfo.setSchemaHash(pair.second);
                        metaInfo.setIsInMemory(isInMemory);
                        metaInfo.setMetaType(metaType);
                        metaInfos.add(metaInfo);
                    }
                } else {
                    // for ReportHandler
                    for (Triple<Long, Integer, Boolean> triple : tabletToInMemory) {
                        TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                        metaInfo.setTabletId(triple.getLeft());
                        metaInfo.setSchemaHash(triple.getMiddle());
                        metaInfo.setIsInMemory(triple.getRight());
                        metaInfo.setMetaType(metaType);
                        metaInfos.add(metaInfo);
                    }
                }
                break;
            }
        }
        updateTabletMetaInfoReq.setTabletMetaInfos(metaInfos);
        return updateTabletMetaInfoReq;
    }
}