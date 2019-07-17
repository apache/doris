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

import java.util.List;
import java.util.Map;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.thrift.TTabletMetaInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUpdateTabletMetaInfoReq;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

public class UpdateTabletMetaInfoTask extends AgentTask {

    private static final Logger LOG = LogManager.getLogger(ClearTransactionTask.class);

    private SetMultimap<Long, Integer> tabletWithoutPartitionId;

    public UpdateTabletMetaInfoTask(long backendId, SetMultimap<Long, Integer> tabletWithoutPartitionId) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO, -1L, -1L, -1L, -1L, -1L, backendId);
        this.tabletWithoutPartitionId = tabletWithoutPartitionId;
    }
    
    public TUpdateTabletMetaInfoReq toThrift() {
        TUpdateTabletMetaInfoReq updateTabletMetaInfoReq = new TUpdateTabletMetaInfoReq();
        List<TTabletMetaInfo> metaInfos = Lists.newArrayList();
        int tabletEntryNum = 0;
        for (Map.Entry<Long, Integer> entry : tabletWithoutPartitionId.entries()) {
            // add at most 10000 tablet meta during one sync to avoid too large task
            if (tabletEntryNum > 10000) {
                break;
            }
            TTabletMetaInfo metaInfo = new TTabletMetaInfo();
            metaInfo.setTablet_id(entry.getKey());
            metaInfo.setSchema_hash(entry.getValue());
            TabletMeta tabletMeta = Catalog.getInstance().getTabletInvertedIndex().getTabletMeta(entry.getKey());
            if (tabletMeta == null) {
                LOG.warn("could not find tablet [{}] in meta ignore it", entry.getKey());
                continue;
            }
            metaInfo.setPartition_id(tabletMeta.getPartitionId());
            metaInfos.add(metaInfo);
            ++tabletEntryNum;
        }
        updateTabletMetaInfoReq.setTabletMetaInfos(metaInfos);
        return updateTabletMetaInfoReq;
    }
}