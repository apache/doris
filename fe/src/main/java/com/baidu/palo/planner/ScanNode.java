// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.planner;

import java.util.List;
import java.util.Map;

import com.baidu.palo.analysis.TupleDescriptor;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TScanRangeLocations;
import com.google.common.base.Objects;

/**
 * Representation of the common elements of all scan nodes.
 */
abstract public class ScanNode extends PlanNode {
    protected final TupleDescriptor desc;
    protected Map<String, PartitionColumnFilter> columnFilters;
    protected String sortColumn = null;

    public ScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc.getId().asList(), planNodeName);
        this.desc = desc;
    }

    /**
     * Helper function to parse a "host:port" address string into TNetworkAddress
     * This is called with ipaddress:port when doing scan range assigment.
     */
    protected static TNetworkAddress addressToTNetworkAddress(String address) {
        TNetworkAddress result = new TNetworkAddress();
        String[] hostPort = address.split(":");
        result.hostname = hostPort[0];
        result.port = Integer.parseInt(hostPort[1]);
        return result;
    }

    public Map<String, PartitionColumnFilter> getColumnFilters() {
        return this.columnFilters;
    }

    public void setColumnFilters(Map<String, PartitionColumnFilter> columnFilters) {
        this.columnFilters = columnFilters;
    }

    public void setSortColumn(String column) {
        sortColumn = column;
    }

    /**
     * Returns all scan ranges plus their locations. Needs to be preceded by a call to
     * finalize().
     *
     * @param maxScanRangeLength The maximum number of bytes each scan range should scan;
     *                           only applicable to HDFS; less than or equal to zero means no
     *                           maximum.
     */
    abstract public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength);

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("tid", desc.getId().asInt()).add("tblName",
                desc.getTable().getName()).add("keyRanges", "").addValue(
                super.debugString()).toString();
    }
}
