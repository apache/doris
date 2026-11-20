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

package org.apache.doris.connector.mongodb;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/** Frozen value partitions. Samples affect balance only; the unbounded ends and complement ensure coverage. */
final class MongoSplitPlan {
    private final String field;
    private final String family;
    private final int requested;
    private final int admitted;
    private final String reason;
    private final List<byte[]> partitions;
    private final long descriptorBudget;

    private MongoSplitPlan(String field, String family, int requested, int admitted, String reason,
            List<BsonDocument> partitions) {
        this.field = field;
        this.family = family;
        this.requested = requested;
        this.admitted = admitted;
        this.reason = reason;
        this.partitions = partitions.stream().map(MongoSplitPlan::encode).toList();
        this.descriptorBudget = 0;
    }

    private MongoSplitPlan(MongoSplitPlan plan, long descriptorBudget) {
        this.field = plan.field;
        this.family = plan.family;
        this.requested = plan.requested;
        this.admitted = plan.admitted;
        this.reason = plan.reason;
        this.partitions = plan.partitions;
        this.descriptorBudget = descriptorBudget;
    }

    MongoSplitPlan withDescriptorBudget(long bytes) {
        return new MongoSplitPlan(this, bytes);
    }

    long descriptorBudget() {
        return descriptorBudget;
    }

    static MongoSplitPlan single(int requested, int admitted, String reason) {
        return new MongoSplitPlan("", "", requested, admitted, reason, List.of(new BsonDocument()));
    }

    static MongoSplitPlan ranges(String field, String family, List<BsonValue> samples, int requested, int admitted) {
        if (!Set.of("integer", "date", "objectId").contains(family)) {
            throw new IllegalArgumentException("Unsupported MongoDB split type family");
        }
        if (!MongoPredicateTranslator.safePath(field) || field.isEmpty() || admitted < 1 || admitted > 256
                || requested < admitted) {
            throw new IllegalArgumentException("Invalid MongoDB split plan");
        }
        if (admitted == 1 || samples.isEmpty()) {
            return single(requested, admitted, admitted == 1
                    ? (requested == 1 ? "requested_one" : "admission_single") : "empty_sample");
        }
        var values = new ArrayList<>(samples);
        Comparator<BsonValue> comparator = comparator(family);
        values.sort(comparator);
        List<BsonValue> boundaries = new ArrayList<>();
        // _id always exists and cannot be an array. Expression comparisons span the complete BSON
        // order, including unsampled types; ordinary query comparisons instead bracket by type.
        // Custom fields can be missing or multikey, so retain their typed ranges and complement.
        boolean wholeId = "_id".equals(field);
        int ranges = wholeId ? admitted : admitted - 1;
        for (int j = 1; j < ranges; j++) {
            BsonValue value = values.get((int) ((long) j * values.size() / ranges));
            if (comparator.compare(value, values.get(0)) > 0
                    && (wholeId || comparator.compare(value, values.get(values.size() - 1)) < 0)
                    && (boundaries.isEmpty() || comparator.compare(value, boundaries.get(boundaries.size() - 1)) > 0)) {
                boundaries.add(value);
            }
        }
        // Without an internal bound, typed ranges plus their complement add a full scan
        // without splitting the useful key space. One complete cursor preserves all BSON types.
        if (boundaries.isEmpty()) {
            return single(requested, admitted, "insufficient_boundaries");
        }
        var types = "integer".equals(family) ? List.of("int", "long") : List.of(family);
        var eligibility = new BsonDocument("$in", new BsonArray(List.of(
                new BsonDocument("$type", new BsonString("$" + field)),
                new BsonArray(types.stream().map(BsonString::new).toList()))));
        List<BsonDocument> partitions = new ArrayList<>();
        for (int i = 0; i <= boundaries.size(); i++) {
            if (wholeId) {
                List<BsonValue> comparisons = new ArrayList<>();
                if (i > 0) {
                    comparisons.add(new BsonDocument("$gte", new BsonArray(List.of(
                            new BsonString("$_id"), boundaries.get(i - 1)))));
                }
                if (i < boundaries.size()) {
                    comparisons.add(new BsonDocument("$lt", new BsonArray(List.of(
                            new BsonString("$_id"), boundaries.get(i)))));
                }
                partitions.add(new BsonDocument("$expr", comparisons.size() == 1 ? comparisons.get(0)
                        : new BsonDocument("$and", new BsonArray(comparisons))));
                continue;
            }
            BsonDocument bounds = new BsonDocument();
            if (i > 0) {
                bounds.put("$gte", boundaries.get(i - 1));
            }
            if (i < boundaries.size()) {
                bounds.put("$lt", boundaries.get(i));
            }
            // Omit infinite ends instead of using MinKey/MaxKey. An empty comparison is true;
            // $expr checks the field itself, whereas query $type would also match array elements.
            var interval = bounds.isEmpty() ? new BsonDocument()
                    : new BsonDocument(field, bounds);
            partitions.add(new BsonDocument("$and", new BsonArray(List.of(
                    new BsonDocument("$expr", eligibility), interval))));
        }
        if (!wholeId) {
            partitions.add(new BsonDocument("$expr", new BsonDocument("$not", new BsonArray(List.of(eligibility)))));
        }
        return new MongoSplitPlan(field, family, requested, admitted, "range", partitions);
    }

    static Comparator<BsonValue> comparator(String family) {
        return switch (family) {
            case "integer" -> Comparator.comparingLong(v -> v.asNumber().longValue());
            case "date" -> Comparator.comparingLong(v -> v.asDateTime().getValue());
            case "objectId" -> (a, b) -> a.asObjectId().getValue().compareTo(b.asObjectId().getValue());
            default -> throw new IllegalArgumentException("Unsupported MongoDB split type family");
        };
    }

    static byte[] encode(BsonDocument value) {
        var buffer = new RawBsonDocument(value, new BsonDocumentCodec()).getByteBuffer().asNIO();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    byte[] partitionBytes(int index) {
        // Thrift descriptors are mutable; each caller owns its bytes while the frozen plan stays private.
        return partitions.get(index).clone();
    }

    BsonDocument partition(int index) {
        return new RawBsonDocument(partitions.get(index)).decode(new BsonDocumentCodec());
    }

    int size() {
        return partitions.size();
    }

    String field() {
        return field;
    }

    String family() {
        return family;
    }

    int requested() {
        return requested;
    }

    int admitted() {
        return admitted;
    }

    String reason() {
        return reason;
    }
}
