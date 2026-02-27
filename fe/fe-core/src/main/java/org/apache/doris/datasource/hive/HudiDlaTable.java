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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.hudi.HudiMvccSnapshot;
import org.apache.doris.datasource.hudi.HudiUtils;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HudiDlaTable extends HMSDlaTable {

    public HudiDlaTable(HMSExternalTable table) {
        super(table);
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return getHudiSchemaCacheValue(snapshot).getPartitionColumns();
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        return true;
    }

    public HMSSchemaCacheValue getHudiSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        long timestamp = 0L;
        if (snapshot.isPresent()) {
            timestamp = ((HudiMvccSnapshot) snapshot.get()).getTimestamp();
        } else {
            timestamp = HudiUtils.getLastTimeStamp(hmsTable);
        }
        return getHudiSchemaCacheValue(timestamp);
    }

    private HMSSchemaCacheValue getHudiSchemaCacheValue(long timestamp) {
        Optional<SchemaCacheValue> schemaCacheValue = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getSchema(hmsTable, timestamp);
        if (!schemaCacheValue.isPresent()) {
            throw new CacheException("failed to getSchema for: %s.%s.%s.%s",
                    null, hmsTable.getCatalog().getName(), hmsTable.getDbName(), hmsTable.getName(), timestamp);
        }
        return (HMSSchemaCacheValue) schemaCacheValue.get();
    }
}
