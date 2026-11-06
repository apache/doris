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

// Explicit BSON null and absent keys must exercise every supported target family independently.
const boundaryFields = ["bool_v", "tiny_v", "small_v", "int_v", "long_v", "large_v", "float_v", "double_v",
    "text_v", "oid_v", "bin_v", "dec32_v", "dec64_v", "dec128_v", "date_v", "date_precision_v", "timestamp_v", "regex_v",
    "array_v", "object_v", "nested_v", "json_v", "decimal_json_v", "min_key_v", "max_key_v", "code_v", "scope_v"];
const allNull = {_id: nativeOid(1), seq: NumberInt(1)};
boundaryFields.forEach(name => allNull[name] = null);
nativeFixture.boundary_records.insertMany([
    allNull, {_id: nativeOid(2), seq: NumberInt(2)},
    {_id: nativeOid(3), seq: NumberInt(3), bool_v: false,
     tiny_v: NumberInt(-128), small_v: NumberInt(-32768), int_v: NumberInt(-2147483648),
     long_v: NumberLong("-9223372036854775808"), large_v: NumberLong("-9223372036854775808"),
     float_v: Double("-3.4028234663852886e38"), double_v: Double("-1.7976931348623157e308"),
     text_v: "", oid_v: ObjectId("000000000000000000000000"), bin_v: BinData(128, ""),
     dec32_v: NumberDecimal("-9999999.99"), dec64_v: NumberDecimal("-99999999999999.9999"),
     dec128_v: NumberDecimal("-9999999999999999999999999999999999"),
     date_v: ISODate("0000-01-01T00:00:00.000Z"), date_precision_v: ISODate("0000-01-01T00:00:00Z"),
     // MongoDB replaces a top-level zero Timestamp with server time; nesting preserves the boundary.
     timestamp_v: {value: Timestamp({t: 0, i: 0})},
     regex_v: /^$/, array_v: [], object_v: {a: null, b: ""}, nested_v: [], json_v: []},
    {_id: nativeOid(4), seq: NumberInt(4), bool_v: true,
     tiny_v: NumberInt(127), small_v: NumberInt(32767), int_v: NumberInt(2147483647),
     long_v: NumberLong("9223372036854775807"), large_v: NumberLong("9223372036854775807"),
     float_v: Double("3.4028234663852886e38"), double_v: Double("1.7976931348623157e308"),
     text_v: "a\u0000中文😀", oid_v: ObjectId("ffffffffffffffffffffffff"), bin_v: BinData(128, "AP+Afw=="),
     dec32_v: NumberDecimal("9999999.99"), dec64_v: NumberDecimal("99999999999999.9999"),
     dec128_v: NumberDecimal("9999999999999999999999999999999999"),
     date_v: ISODate("9999-12-31T23:59:59.999Z"), date_precision_v: ISODate("9999-12-31T23:59:59Z"),
     timestamp_v: {value: Timestamp({t: 4294967295, i: 4294967295})},
     regex_v: /中文.*é/im, array_v: [null, NumberInt(-2147483648), NumberInt(2147483647)],
     object_v: {a: NumberInt(2147483647), b: "中文"},
     nested_v: [null, {a: null, b: ""}, {a: NumberInt(2147483647), b: null}], json_v: {},
     min_key_v: MinKey(), max_key_v: MaxKey(), code_v: Code("return 1"), scope_v: Code("return n", {n: null})},
    {_id: nativeOid(5), seq: NumberInt(5), bool_v: false, tiny_v: NumberInt(0), small_v: NumberInt(0),
     int_v: NumberInt(0), long_v: NumberLong("0"), large_v: NumberLong("0"),
     float_v: Double("1.401298464324817e-45"), double_v: Double("5e-324"), bin_v: BinData(128, "AA=="),
     text_v: "a".repeat(4096), dec32_v: NumberDecimal("-0.00"), dec64_v: NumberDecimal("0.0000"),
     dec128_v: NumberDecimal("0E-6176"), date_v: new Date(-1), date_precision_v: new Date(0),
     array_v: [null], object_v: {},
     nested_v: [null, {a: null, b: null}], json_v: ""},
    {_id: nativeOid(6), seq: NumberInt(6), float_v: Double("NaN"), double_v: Double("NaN"),
     decimal_json_v: NumberDecimal("NaN")},
    {_id: nativeOid(7), seq: NumberInt(7), float_v: Double("Infinity"), double_v: Double("Infinity"),
     decimal_json_v: NumberDecimal("9.999999999999999999999999999999999E+6144")},
    {_id: nativeOid(8), seq: NumberInt(8), float_v: Double("-Infinity"), double_v: Double("-Infinity"),
     decimal_json_v: NumberDecimal("1E-6176")},
    {_id: nativeOid(9), seq: NumberInt(9), float_v: Double("-0.0"), double_v: Double("-0.0"),
     decimal_json_v: NumberDecimal("-0.0000")},
    {_id: nativeOid(10), seq: NumberInt(10), float_v: Double("1.1754943508222875e-38"),
     double_v: Double("2.2250738585072014e-308"), dec32_v: NumberDecimal("-0.01"),
     dec64_v: NumberDecimal("0.0001"), dec128_v: NumberDecimal("0.0001"), date_v: new Date(0),
     code_v: Code(""), scope_v: Code("", {})},
    {_id: nativeOid(11), seq: NumberInt(11), float_v: Double("0.0"), double_v: Double("0.0")}
]);

// Values just beyond target ranges and values requiring coercion/rounding must fail atomically.
nativeFixture.boundary_errors.insertMany([
    {_id: nativeOid(1), seq: NumberInt(1), bool_v: NumberInt(1), tiny_v: NumberInt(128),
     small_v: NumberInt(32768), int_v: NumberLong("2147483648"), long_v: Double("9007199254740992"),
     large_v: "9223372036854775808", float_v: Double("0.1"), text_v: NumberInt(1),
     oid_v: BinData(0, "AAAAAAAAAAAAAAAA"), bin_v: BinData(0, "AA=="),
     dec32_v: NumberDecimal("10000000.00"), dec64_v: NumberDecimal("100000000000000.0000"),
     dec128_v: NumberDecimal("1E+34"), date_v: new Date(253402300800000), date_precision_v: new Date(-1),
     timestamp_v: NumberLong("1"), regex_v: "pattern", array_v: [NumberInt(1), "bad"],
     object_v: {a: NumberInt(1), b: NumberInt(2)}, nested_v: [{a: NumberInt(1)}, {a: "bad"}]},
    {_id: nativeOid(2), seq: NumberInt(2), tiny_v: NumberInt(-129), small_v: NumberInt(-32769),
     int_v: NumberLong("-2147483649"), float_v: Double("1e300"),
     dec32_v: NumberDecimal("1.001"), dec64_v: NumberDecimal("0.00001"),
     dec128_v: NumberDecimal("1E+6144"), date_v: new Date(-62167219200001), array_v: true, object_v: 1},
    {_id: nativeOid(3), seq: NumberInt(3), dec32_v: NumberDecimal("NaN"),
     dec64_v: NumberDecimal("Infinity"), dec128_v: NumberDecimal("-Infinity")}
]);
// MongoDB permits this depth, while the native reader's declared decoding limit is lower.
let deepValue = NumberInt(1);
for (let i = 0; i < 65; ++i) deepValue = [deepValue];
nativeFixture.deep_limit_records.insertOne({_id: nativeOid(1), value: deepValue});

// Legal validators outside the typed mapper's grammar must retain every value in document mode.
const fallbackSchemas = {
    tuple_validator: {bsonType: "object", properties: {values: {bsonType: "array",
        items: [{bsonType: "int"}, {bsonType: "string"}]}}},
    pattern_validator: {bsonType: "object", properties: {known: {bsonType: "int"}},
        patternProperties: {"^extra$": {bsonType: "string"}}},
    dependency_validator: {bsonType: "object", properties: {known: {bsonType: "int"}},
        dependencies: {known: {properties: {extra: {bsonType: "string"}}}}},
    nested_pattern_validator: {bsonType: "object", properties: {profile: {bsonType: "object",
        properties: {known: {bsonType: "int"}}, patternProperties: {"^extra$": {bsonType: "string"}}}}}
};
Object.entries(fallbackSchemas).forEach(([name, schema]) => {
    nativeFixture.createCollection(name, {validator: {$jsonSchema: schema}});
    nativeFixture.getCollection(name).insertOne({_id: nativeOid(1), known: NumberInt(1), extra: "present",
        values: [NumberInt(1), "x"], profile: {known: NumberInt(2), extra: "nested"}});
});
// Whole-document validation must use depth-bounded scratch even for a narrow scalar projection.
nativeFixture.wide_records.insertOne({_id: nativeOid(1), seq: NumberInt(1),
    wide: Array.from({length: 900000}, () => ({}))});
// Cross the driver's 128-name metadata batch without enlarging the normal test database.
const paginationFixture = db.getSiblingDB("doris_mongodb_pagination");
for (let i = 0; i < 140; ++i) {
    paginationFixture.createCollection("page_" + i.toString().padStart(3, "0"));
}
