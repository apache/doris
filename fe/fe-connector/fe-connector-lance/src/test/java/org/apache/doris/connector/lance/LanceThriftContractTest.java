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

package org.apache.doris.connector.lance;

import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * The range descriptor the plugin hands BE survives the compact protocol the scan ranges travel in, with the
 * optional fields set exactly when the plan needs them (branch-4.2's LanceThriftContractTest).
 */
class LanceThriftContractTest {
    private static final String URI = "s3://warehouse/db/table.lance";

    @Test
    void scalarIndexFieldsSurviveTheCompactProtocol() throws Exception {
        UUID segment = new UUID(1, 2);
        for (LanceScanRange range : Arrays.asList(
                LanceScanRange.indexSegment(URI, 42, segment, Arrays.asList(7L, 11L), 10, 10, -1),
                LanceScanRange.fragments(URI, 42, Arrays.asList(7L, 11L), 10, 10, true, -1))) {
            boolean indexed = range.getIndexSegmentUuid().isPresent();
            TTableFormatFileDesc restored = roundTrip(range);
            TLanceFileDesc params = restored.getLanceParams();
            Assertions.assertEquals(indexed, params.isSetIndexSegmentUuids());
            Assertions.assertEquals(!indexed, params.isSetUseScalarIndex());
            if (indexed) {
                ByteBuffer uuid = params.getIndexSegmentUuids().get(0).duplicate();
                Assertions.assertEquals(segment, new UUID(uuid.getLong(), uuid.getLong()));
            } else {
                Assertions.assertFalse(params.isUseScalarIndex());
            }
        }
    }

    @Test
    void limitIsSetOnlyWhenPushed() throws Exception {
        TLanceFileDesc limited = roundTrip(LanceScanRange.fragments(URI, 42, Arrays.asList(7L, 11L), 10, 10, false,
                100)).getLanceParams();
        Assertions.assertEquals(URI, limited.getDatasetUri());
        Assertions.assertEquals(Arrays.asList(7L, 11L), limited.getFragmentIds());
        Assertions.assertEquals(42L, limited.getVersion());
        Assertions.assertTrue(limited.isSetLimit());
        Assertions.assertEquals(100L, limited.getLimit());
        // A scan without a pushable LIMIT must leave the field unset so the BE reads all rows.
        Assertions.assertFalse(roundTrip(LanceScanRange.fragments(URI, 1, Arrays.asList(1L), 10, 10, false, -1))
                .getLanceParams().isSetLimit());
        Assertions.assertEquals(19, TFileFormatType.FORMAT_LANCE.getValue());
    }

    private static TTableFormatFileDesc roundTrip(LanceScanRange range) throws Exception {
        TTableFormatFileDesc source = new TTableFormatFileDesc();
        range.populateRangeParams(source, new TFileRangeDesc());
        TTableFormatFileDesc restored = new TTableFormatFileDesc();
        new TDeserializer(new TCompactProtocol.Factory()).deserialize(restored,
                new TSerializer(new TCompactProtocol.Factory()).serialize(source));
        Assertions.assertEquals(source, restored);
        return restored;
    }
}
