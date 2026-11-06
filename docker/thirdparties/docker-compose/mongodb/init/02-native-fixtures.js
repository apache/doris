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

const nativeFixture = db.getSiblingDB("doris_mongodb_native");
nativeFixture.dropDatabase();
const nativeOid = n => ObjectId(n.toString(16).padStart(24, "0"));
nativeFixture.createCollection("typed_records", {validator: {$jsonSchema: {
    bsonType: "object", required: ["seq"], properties: {
        _id: {bsonType: "objectId"}, seq: {bsonType: "long"}, name: {bsonType: "string"},
        grp: {bsonType: "string"}, score: {bsonType: ["int", "long", "null"]},
        active: {bsonType: "bool"}, ratio: {bsonType: "double"},
        amount: {bsonType: "decimal"}, created: {bsonType: "date"},
        tags: {bsonType: "array", items: {bsonType: ["string", "null"]}},
        profile: {bsonType: "object", properties: {
            City: {bsonType: "string"}, rank: {bsonType: "long"}, manager: {bsonType: "objectId"}
        }},
        clock: {bsonType: "timestamp"}, blob: {bsonType: "binData"}
    }
}}});
// mongosh encodes integral JavaScript numbers as Int32; Double keeps the validator's physical tag.
nativeFixture.typed_records.insertMany([
    {_id: nativeOid(1), seq: NumberLong("1"), name: "alpha", grp: "east", score: NumberInt(10),
     active: true, ratio: 1.25, amount: NumberDecimal("123.4500"), created: new Date(-1),
     tags: ["red", "blue"], profile: {City: "A", rank: NumberLong("1"), manager: nativeOid(101)},
     clock: Timestamp({t: 4294967295, i: 4294967295}), blob: BinData(0, "AP+A")},
    {_id: nativeOid(2), seq: NumberLong("2"), name: "beta", grp: "west", score: NumberLong("20"),
     active: false, ratio: -0.5, amount: NumberDecimal("-2.50"), created: ISODate("2024-02-01T12:34:56.789Z"),
     tags: [], profile: {City: "B", rank: NumberLong("2")}, blob: BinData(128, "AP+A")},
    {_id: nativeOid(3), seq: NumberLong("3"), name: "中文-é", grp: "east", score: null,
     active: true, ratio: Double(0.0), amount: NumberDecimal("0E-6176"), created: new Date(0), tags: [null, "中文"]},
    {_id: nativeOid(4), seq: NumberLong("4"), name: "delta", grp: "west", active: false,
     ratio: 2.5, amount: NumberDecimal("99999.99"), created: ISODate("2038-01-19T03:14:07.000Z")}
]);
nativeFixture.createCollection("empty_records", {validator: {$jsonSchema: {
    bsonType: "object", properties: {_id: {bsonType: "objectId"}, seq: {bsonType: "long"}}
}}});
nativeFixture.createCollection("large_records", {validator: {$jsonSchema: {
    bsonType: "object", properties: {_id: {bsonType: "objectId"}, seq: {bsonType: "long"}, payload: {bsonType: "string"}}
}}});
nativeFixture.large_records.insertMany(Array.from({length: 2081}, (_, i) => ({
    _id: nativeOid(i + 1), seq: NumberLong(String(i)), payload: "payload_" + i % 7
})));
// The view removes _id and computes output fields. Reading its source would return different rows.
nativeFixture.createView("record_view", "typed_records", [
    {$match: {active: true}}, {$project: {_id: 0, seq: 1, label: {$concat: ["view-", "$name"]}}}
], {collation: {locale: "en", strength: 2}});
nativeFixture.createCollection("measurements", {timeseries: {timeField: "observed", metaField: "device"}});
nativeFixture.measurements.insertMany([
    {observed: ISODate("2024-01-01T00:00:00.001Z"), device: "a", value: NumberInt(1)},
    {observed: ISODate("2024-01-01T00:00:00.001Z"), device: "a", value: NumberInt(2)},
    {observed: ISODate("2024-01-02T00:00:00.000Z"), device: "b", value: NumberInt(3)}
]);
nativeFixture.special_records.insertOne({
    _id: nativeOid(1), integral: NumberLong("9007199254740993"), decimal: NumberDecimal("1.2300"),
    date: new Date(-1), bytes: BinData(128, "AP+A"), pattern: /a.*b/im,
    clock: Timestamp({t: 4294967295, i: 7}), low: MinKey(), high: MaxKey(),
    code: Code("return n", {n: NumberInt(1)}), refs: [nativeOid(2), null],
    "literal.dot": "literal", nested: {dot: "nested"}, "dollar$field": "dollar", "unicode": "中文"
});
nativeFixture.mismatched.insertMany([
    {_id: nativeOid(1), seq: NumberLong("1"), n: NumberLong("10"), arr: [NumberInt(1), NumberInt(2)],
     obj: {a: NumberInt(1), b: "ok"}, amount: NumberDecimal("1.2300"), blob: BinData(0, "AA==")},
    {_id: nativeOid(2), seq: NumberLong("2"), n: "bad", arr: [NumberInt(3), "bad"],
     obj: {a: NumberInt(2), b: NumberInt(3)}, amount: NumberDecimal("1.234"), blob: BinData(128, "AA==")},
    {_id: nativeOid(3), seq: NumberLong("3"), n: NumberLong("30"), arr: [NumberInt(4)],
     obj: {a: NumberInt(3)}, amount: NumberDecimal("2.00"), blob: BinData(0, "")}
]);
// A query-only validator cannot be represented as a complete column schema: auto must use document mode.
nativeFixture.createCollection("unsupported_validator", {validator: {value: {$gte: 0}}});
nativeFixture.unsupported_validator.insertOne({_id: nativeOid(1), value: NumberInt(1)});
// Existing documents may violate a validator introduced after insertion.
nativeFixture.historical_records.insertOne({seq: "historical-string"});
nativeFixture.runCommand({collMod: "historical_records", validator: {$jsonSchema: {
    bsonType: "object", properties: {seq: {bsonType: "long"}}
}}, validationLevel: "moderate"});
nativeFixture.createRole({role: "nativeViewReader", privileges: [
    {resource: {db: "doris_mongodb_native", collection: "record_view"}, actions: ["find"]}
], roles: []});
nativeFixture.createUser({user: "native_view_reader", pwd: "native_view_reader",
    roles: [{role: "nativeViewReader", db: "doris_mongodb_native"}]});
