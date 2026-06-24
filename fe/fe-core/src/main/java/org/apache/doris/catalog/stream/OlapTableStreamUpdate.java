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

package org.apache.doris.catalog.stream;

import org.apache.doris.common.UserException;
import org.apache.doris.transaction.TransactionCommitFailedException;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;

public class OlapTableStreamUpdate extends AbstractTableStreamUpdate {
    // previous partition consumed version
    @SerializedName(value = "prev")
    private final Map<Long, Long> prev;
    // new partition consumed version
    @SerializedName(value = "next")
    private final Map<Long, Long> next;

    public OlapTableStreamUpdate() {
        this.prev = new HashMap<>();
        this.next = new HashMap<>();
    }

    public OlapTableStreamUpdate(Map<Long, Long> prev, Map<Long, Long> next) {
        this.prev = prev;
        this.next = next;
    }

    public Map<Long, Long> getNext() {
        return next;
    }

    public Map<Long, Long> getPrev() {
        return prev;
    }

    @Override
    public void merge(AbstractTableStreamUpdate other) {
        Preconditions.checkArgument(other instanceof OlapTableStreamUpdate);
        this.next.putAll(((OlapTableStreamUpdate) other).getNext());
        this.prev.putAll(((OlapTableStreamUpdate) other).getPrev());
    }

    public void checkPartitionOffset(String dbName, String streamName, Map<Long, Long> historicalPartitionOffset,
                                     Map<Long, Long> partitionOffset)
            throws UserException {
        for (Map.Entry<Long, Long> entry : next.entrySet()) {
            if (historicalPartitionOffset.containsKey(entry.getKey())) {
                if (!historicalPartitionOffset.get(entry.getKey()).equals(entry.getValue())) {
                    throw new TransactionCommitFailedException("history offset already consumed: "
                            + dbName + '-' + streamName + '-' + entry.getKey() + '-' + entry.getValue()
                            + " vs "  + historicalPartitionOffset.get(entry.getKey()));
                }
            } else if (partitionOffset.containsKey(entry.getKey())) {
                if (!prev.containsKey(entry.getKey())) {
                    throw new TransactionCommitFailedException(
                            "previous version missing for partition=" + entry.getKey() + ", db=" + dbName
                                    + ", stream=" + streamName);
                }
                if (!partitionOffset.get(entry.getKey()).equals(prev.get(entry.getKey()))) {
                    throw new TransactionCommitFailedException("target offset already consumed: "
                            + dbName + '-' + streamName + '-' + entry.getKey() + '-' + entry.getValue()
                            + " vs "  + partitionOffset.get(entry.getKey()));
                }
            } else {
                // the new partition id may not be updated in time, so key in table stream not exist is also valid
            }
        }
    }
}
