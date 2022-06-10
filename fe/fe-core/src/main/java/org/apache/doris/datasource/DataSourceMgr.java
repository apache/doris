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

package org.apache.doris.datasource;

import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * DataSourceMgr will loaded all data sources at FE startup,
 * and save them in maps mapping with id and name.
 */
public class DataSourceMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(DataSourceMgr.class);

    private Map<Long, DataSourceIf> idToDataSource = Maps.newConcurrentMap();
    private Map<String, DataSourceIf> nameToDataSource = Maps.newConcurrentMap();
    private DataSourceMgrProperty dsMgrProperty = new DataSourceMgrProperty();

    // Use a separate instance to facilitate access.
    // internalDataSource still exists in idToDataSource and nameToDataSource
    private InternalDataSource internalDataSource;

    public DataSourceMgr() {
        initInternalDataSource();
    }

    private void initInternalDataSource() {
        internalDataSource = new InternalDataSource();
        idToDataSource.put(internalDataSource.getId(), internalDataSource);
        nameToDataSource.put(internalDataSource.getName(), internalDataSource);
    }

    private void registerNewDataSource(ExternalDataSource ds) {
        // TODO
    }

    public InternalDataSource getInternalDataSource() {
        return internalDataSource;
    }

    /**
     * get data source by id.
     *
     * @param id
     * @param e
     * @param <E>
     * @return
     * @throws E
     */
    public <E extends MetaNotFoundException> DataSourceIf getDataSourceOrException(long id,
            java.util.function.Function<Long, E> e) throws E {
        DataSourceIf ds = idToDataSource.get(id);
        if (ds == null) {
            throw e.apply(id);
        }
        return ds;
    }

    /**
     * get data source by name.
     *
     * @param name
     * @param e
     * @param <E>
     * @return
     * @throws E
     */
    public <E extends MetaNotFoundException> DataSourceIf getDataSourceOrException(String name,
            java.util.function.Function<String, E> e) throws E {
        DataSourceIf ds = nameToDataSource.get(name);
        if (ds == null) {
            throw e.apply(name);
        }
        return ds;
    }

    public boolean hasDataSource(String name) {
        return nameToDataSource.containsKey(name);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (Config.disable_cluster_feature) {
            return;
        }
        Preconditions.checkState(false, "Do not call this until multi catalog feature is ready");
        int size = idToDataSource.size();
        if (idToDataSource.get(InternalDataSource.INTERNAL_DS_ID) != null) {
            // No need to persis internal data source
            size -= 1;
        }
        out.writeInt(size);
        for (DataSourceIf ds : idToDataSource.values()) {
            if (ds.getId() == InternalDataSource.INTERNAL_DS_ID) {
                continue;
            }
            ExternalDataSource extDs = (ExternalDataSource) ds;
            extDs.write(out);
        }
        dsMgrProperty.write(out);
    }

    /**
     * read from image.
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static DataSourceMgr read(DataInput in) throws IOException {
        if (Config.disable_cluster_feature) {
            return null;
        }
        DataSourceMgr mgr = new DataSourceMgr();
        mgr.readFields(in);
        return mgr;
    }

    private void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            ExternalDataSource extDs = ExternalDataSource.read(in);
            idToDataSource.put(extDs.getId(), extDs);
            nameToDataSource.put(extDs.getName(), extDs);
        }
        dsMgrProperty = DataSourceMgrProperty.read(in);
    }
}
