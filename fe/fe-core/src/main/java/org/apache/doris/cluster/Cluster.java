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

package org.apache.doris.cluster;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Deprecated
public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    private Long id;
    private String name;
    // backend which cluster own
    private Set<Long> backendIdSet = ConcurrentHashMap.newKeySet();
    private Set<Long> dbIds = ConcurrentHashMap.newKeySet();
    private Set<String> dbNames = ConcurrentHashMap.newKeySet();

    private Cluster() {
        // for persist
    }

    public Cluster(String name, long id) {
        this.name = name;
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public static Cluster read(DataInput in) throws IOException {
        Cluster cluster = new Cluster();
        cluster.readFields(in);
        return cluster;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("Cluster should not be persisted anymore");
    }

    private void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);
        Long len = in.readLong();
        while (len-- > 0) {
            Long id = in.readLong();
            backendIdSet.add(id);
        }
        int count = in.readInt();
        while (count-- > 0) {
            dbNames.add(Text.readString(in));
        }

        count = in.readInt();
        while (count-- > 0) {
            dbIds.add(in.readLong());
        }

        // for linkDbNames
        in.readInt();
        // for linkDbIds
        in.readInt();
    }
}
