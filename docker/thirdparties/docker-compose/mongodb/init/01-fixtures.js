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

const fixture = db.getSiblingDB("doris_mongodb_test");
fixture.dropDatabase();
const oid = n => ObjectId(n.toString(16).padStart(24, "0"));
const special = ObjectId("00ff807f0100020304050607");
function schema(table, fields, collection = "_schema") {
    fixture.getCollection(collection).insertOne({table, fields: [
        {name: "_id", type: "ObjectId", hidden: true},
        ...fields.map(([name, type]) => ({name, type, hidden: false}))
    ]});
}

fixture.records.insertMany([
    {_id: oid(1), seq: NumberLong("1"), name: "alpha", grp: "east", active: true, score: NumberInt(10),
     amount: NumberDecimal("123.45"), ratio: 1.25, created: ISODate("2024-01-01T00:00:00.001Z"),
     owner: oid(101), blob: BinData(0, "AP+A"), tags: ["red", "blue"], numbers: [NumberLong("1"), NumberLong("2")],
     profile: {city: "A", rank: NumberLong("1"), manager: special}, refs: [oid(101), special], optional_note: "first"},
    {_id: oid(2), seq: NumberLong("2"), name: "beta", grp: "west", active: false, score: NumberLong("20"),
     amount: NumberDecimal("-2.50"), ratio: -0.5, created: ISODate("2024-02-01T12:34:56.789Z"),
     owner: oid(102), blob: BinData(0, ""), tags: [], numbers: [],
     profile: {city: "B", rank: NumberLong("2"), manager: oid(102)}, refs: [], optional_note: null},
    {_id: oid(3), seq: NumberLong("3"), name: "gamma", grp: "east", active: true, score: null,
     amount: NumberDecimal("0.00"), ratio: 0.0, created: ISODate("1970-01-01T00:00:00.000Z"),
     owner: null, blob: null, tags: ["red", null], numbers: [NumberLong("-1"), null],
     profile: {city: "C", rank: null, manager: null}, refs: [null, oid(101)]},
    {_id: oid(4), seq: NumberLong("4"), name: "delta", grp: "west", active: false,
     amount: NumberDecimal("99999.99"), ratio: 2.5, created: ISODate("2038-01-19T03:14:07.000Z"),
     tags: null, numbers: null, profile: null, refs: null},
    {_id: oid(5), seq: NumberLong("5"), name: "epsilon", grp: "east", active: true, score: NumberLong("-5"),
     amount: NumberDecimal("5.05"), ratio: 3.5, created: ISODate("2024-01-03T00:00:00.000Z"),
     owner: oid(101), blob: BinData(0, "YWJj"), tags: ["green"], numbers: [NumberLong("9223372036854775807")],
     profile: {city: "A", rank: NumberLong("5"), manager: oid(101)}, refs: [special]},
    {_id: ObjectId("ffffffffffffffffffffffff"), seq: NumberLong("6"), name: "中文-é", grp: "north", active: true,
     score: NumberLong("0"), amount: NumberDecimal("1.00"), ratio: 4.5, created: null,
     owner: special, blob: BinData(0, "/wA="), tags: ["unicode", "中文"], numbers: [NumberLong("0")],
     profile: {city: "D", rank: NumberLong("6"), manager: special}, refs: [special, special]}
]);
schema("records", [["seq", "bigint"], ["name", "varchar"], ["grp", "varchar"], ["active", "boolean"],
    ["score", "bigint"], ["amount", "decimal(18,2)"], ["ratio", "double"], ["created", "timestamp(3)"],
    ["owner", "ObjectId"], ["blob", "varbinary"], ["tags", "array(varchar)"], ["numbers", "array(bigint)"],
    ["profile", 'row("city" varchar, "rank" bigint, "manager" ObjectId)'],
    ["refs", "array(ObjectId)"], ["optional_note", "varchar"]]);
schema("records", [["seq", "bigint"], ["name", "varchar"]], "_schema_alternate");

fixture.owners.insertMany([
    {_id: oid(101), label: "team-a", weight: NumberLong("10")},
    {_id: oid(102), label: "team-b", weight: NumberLong("20")},
    {_id: special, label: "team-c", weight: NumberLong("30")}
]);
schema("owners", [["label", "varchar"], ["weight", "bigint"]]);
fixture.createCollection("empty_records");
schema("empty_records", [["seq", "bigint"], ["owner", "ObjectId"]]);
fixture.inferred_records.insertMany([{_id: oid(201), seq: NumberLong("1"), label: "auto"},
    {_id: oid(202), seq: NumberLong("2"), label: "inferred"}]);
fixture.mixed_values.insertMany([{_id: oid(301), seq: NumberLong("1"), value: NumberLong("7")},
    {_id: oid(302), seq: NumberLong("2"), value: "wrong-type"},
    {_id: oid(303), seq: NumberLong("3"), value: null}, {_id: oid(304), seq: NumberLong("4")}]);
schema("mixed_values", [["seq", "bigint"], ["value", "bigint"]]);
const bulk = [];
for (let i = 0; i < 2081; i++) {
    bulk.push({_id: oid(1000 + i), seq: NumberLong(String(i)), bucket: NumberLong(String(i % 7)),
        owner: i % 7 === 0 ? null : special, payload: "row-" + i,
        json_payload: i % 7 === 0 ? null : {seq: i, text: "row-" + i, nested: {active: i % 2 === 0}}});
}
fixture.large_records.insertMany(bulk);
schema("large_records", [["seq", "bigint"], ["bucket", "bigint"], ["owner", "ObjectId"],
    ["payload", "varchar"], ["json_payload", "json"]]);
fixture.invalid_schema.insertOne({_id: oid(401), payload: "unsupported"});
schema("invalid_schema", [["payload", "interval day to second"]]);

fixture.json_records.insertMany([
    {_id: oid(601), seq: NumberLong("1"),
     payload: {city: "北京", n: NumberInt(7), active: true, items: [1, "two", null],
         nested: {key: "value"}, empty_object: {}, empty_array: [], owner: oid(101)},
     json_array: [{x: 1}, '"text"', "null", null, "[1,2]"],
     json_row: {payload: {x: 9}}, json_map: {a: {x: 3}, b: "null", c: null}},
    {_id: oid(602), seq: NumberLong("2"), payload: '[1,"two",null,{"ok":true}]',
     json_array: [], json_row: null, json_map: {}},
    {_id: oid(603), seq: NumberLong("3"), payload: JSON.stringify('中文-é"\\\n\u0000'),
     json_array: null, json_row: {payload: null}, json_map: null},
    {_id: oid(604), seq: NumberLong("4"), payload: "true"},
    {_id: oid(605), seq: NumberLong("5"), payload: "-9223372036854775808",
     json_array: ['"123"', '"null"', '"true"', JSON.stringify('x"y\\z\n中文'), "123", "true"],
     json_row: {payload: '"123"'},
     json_map: {number_string: '"123"', null_string: '"null"', bool_string: '"true"',
         escaped: JSON.stringify('x"y\\z\n中文')}},
    {_id: oid(606), seq: NumberLong("6"), payload: "123.125"},
    {_id: oid(607), seq: NumberLong("7"), payload: "null"},
    {_id: oid(608), seq: NumberLong("8"), payload: null},
    {_id: oid(609), seq: NumberLong("9")},
    {_id: oid(610), seq: NumberLong("10"), payload: {}},
    {_id: oid(611), seq: NumberLong("11"), payload: "[]"},
    {_id: oid(612), seq: NumberLong("12"), payload: [{x: 1}, {x: 2}]}
]);
schema("json_records", [["seq", "bigint"], ["payload", "json"], ["json_array", "array(json)"],
    ["json_row", 'row("payload" json)'], ["json_map", "map(varchar,json)"]]);
fixture.createCollection("empty_json_records");
schema("empty_json_records", [["seq", "bigint"], ["payload", "json"], ["json_array", "array(json)"]]);
fixture.invalid_json_records.insertOne({_id: oid(620), seq: NumberLong("1"), payload: "{"});
schema("invalid_json_records", [["seq", "bigint"], ["payload", "json"]]);

fixture.readonly_inferred_records.insertOne({_id: oid(501), seq: NumberLong("1")});
db.getSiblingDB("admin").createUser({user: "mongodb_readonly", pwd: "mongodb_readonly",
    roles: [{role: "read", db: "doris_mongodb_test"}]});

fixture.numeric_limits.insertMany([
    {_id: ObjectId("000000000000000000000000"), seq: NumberLong("1"), i32: NumberInt(2147483647),
     i64: NumberLong("9223372036854775807"), amount: NumberDecimal("999999999999999999999999.1234567890"),
     ratio: 1.0e100, text: "", created: ISODate("1969-12-31T23:59:59.999Z")},
    {_id: ObjectId("ffffffffffffffffffffffff"), seq: NumberLong("2"), i32: NumberInt(-2147483648),
     i64: NumberLong("-9223372036854775808"), amount: NumberDecimal("-999999999999999999999999.1234567890"),
     ratio: -1.0e100, text: "a\\b\"c", created: ISODate("1900-01-01T00:00:00.000Z")}
]);
schema("numeric_limits", [["seq", "bigint"], ["i32", "bigint"], ["i64", "bigint"],
    ["amount", "decimal(38,10)"], ["ratio", "double"], ["text", "varchar"], ["created", "timestamp(3)"]]);
