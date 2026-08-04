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

package org.apache.doris.transaction;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.List;

/**
 * Serializes connector-specific Thrift commit fragments produced by BE and feeds
 * them, one fragment at a time, into a {@link Transaction} through
 * {@link Transaction#addCommitData(byte[])}.
 *
 * <p>This is the single place the FE-side serialization protocol is defined. It
 * MUST match the deserialization protocol used by the write transactions'
 * {@code addCommitData} overrides (maxcompute / hive / iceberg); the
 * {@code CommitDataSerializerTest} golden tests pin that agreement.</p>
 */
public final class CommitDataSerializer {

    private CommitDataSerializer() {
    }

    /**
     * Serializes each commit fragment and accumulates it into {@code txn}.
     *
     * @param txn       the transaction collecting commit data for this write
     * @param fragments connector-specific Thrift commit fragments, one per BE write fragment
     */
    public static void feed(Transaction txn, List<? extends TBase<?, ?>> fragments) {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            for (TBase<?, ?> fragment : fragments) {
                txn.addCommitData(serializer.serialize(fragment));
            }
        } catch (TException e) {
            throw new RuntimeException("failed to serialize connector commit data", e);
        }
    }
}
