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

package org.apache.doris.persist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.doris.catalog.Database.DbState;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

public class DropLinkDbAndUpdateDbInfo implements Writable {

    private DbState state;
    private LinkDbInfo linkDbInfo;

    public DropLinkDbAndUpdateDbInfo() {
        state = DbState.NORMAL;
        linkDbInfo = new LinkDbInfo();
    }

    public void setUpdateDbState(DbState dbState) {
        this.state = dbState;
    }

    public DbState getUpdateDbState() {
        return state;
    }

    public void setDropDbCluster(String cluster) {
        this.linkDbInfo.setCluster(cluster);
    }

    public String getDropDbCluster() {
        return linkDbInfo.getCluster();
    }

    public void setDropDbName(String db) {
        this.linkDbInfo.setName(db);
    }

    public String getDropDbName() {
        return linkDbInfo.getName();
    }

    public void setDropDbId(long id) {
        this.linkDbInfo.setId(id);
    }

    public long getDropDbId() {
        return linkDbInfo.getId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, state.toString());
        linkDbInfo.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        state = DbState.valueOf(Text.readString(in));
        linkDbInfo.readFields(in);
    }

}
