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

package org.apache.doris.paimon;

import org.apache.paimon.bucket.DefaultBucketFunction;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.sink.ChannelComputer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

/**
 * SDK side of the fixed-bucket native routing golden test.
 *
 * <p>The values in this test intentionally match {@code PaimonNativeRowHashTest}. If a Paimon
 * upgrade changes BinaryRow, DefaultBucketFunction, or ChannelComputer, the Java test exposes the
 * new SDK result while the BE test protects the native implementation from silently diverging.
 */
class PaimonNativeRoutingGoldenTest {

    @Test
    void testBinaryRowHashGoldenValues() {
        Assertions.assertEquals(-1670924195, BinaryRow.EMPTY_ROW.hashCode());
        Assertions.assertEquals(1465514398, row(1, writer -> writer.writeInt(0, 1)).hashCode());
        Assertions.assertEquals(-1748325344, row(1, writer -> writer.setNullAt(0)).hashCode());
        Assertions.assertEquals(-843760178, row(1, writer -> writer.writeString(
                0, BinaryString.fromString("abcdefgh"))).hashCode());
        Assertions.assertEquals(-101922419, row(1, writer -> writer.writeString(
                0, BinaryString.fromString("abc"))).hashCode());
        Assertions.assertEquals(261371745, row(2, writer -> {
            writer.writeInt(0, 1);
            writer.writeString(1, BinaryString.fromString("abc"));
        }).hashCode());
    }

    @Test
    void testBucketAndWriterOwnershipGoldenValues() {
        DefaultBucketFunction bucketFunction = new DefaultBucketFunction();
        BinaryRow bucketKey = row(1, writer -> writer.writeInt(0, 1));
        Assertions.assertEquals(2, bucketFunction.bucket(bucketKey, 4));

        assertChannels(BinaryRow.EMPTY_ROW, 2, new int[][] {
                {1, 0}, {2, 1}, {3, 1}, {8, 5}
        });
        assertChannels(bucketKey, 1, new int[][] {
                {1, 0}, {2, 1}, {3, 2}, {4, 3}, {8, 7}
        });
        BinaryRow stringPartition = row(1, writer -> writer.writeString(
                0, BinaryString.fromString("abc")));
        assertChannels(stringPartition, 3, new int[][] {
                {1, 0}, {2, 0}, {3, 2}, {4, 2}, {8, 6}
        });
    }

    private static BinaryRow row(int arity, Consumer<BinaryRowWriter> values) {
        BinaryRow row = new BinaryRow(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        values.accept(writer);
        writer.complete();
        return row;
    }

    private static void assertChannels(BinaryRow partition, int bucket, int[][] expected) {
        for (int[] item : expected) {
            int writers = item[0];
            int owner = item[1];
            Assertions.assertEquals(owner, ChannelComputer.select(partition, bucket, writers),
                    "writers=" + writers);
        }
    }
}
