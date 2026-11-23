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

const m1 = db.getSiblingDB("doris_mongodb_m1");
m1.dropDatabase();
m1.records.insertMany([
    {seq: NumberLong("1"), n: NumberLong("9007199254740993"), text: "alpha", active: true,
     amount: NumberDecimal("123.4500"), observed: new Date(-1), bytes: BinData(0, "AP+A"),
     nested: [{amount: NumberDecimal("1.20"), tags: ["a", null]}, null, {}]},
    {seq: NumberLong("2"), n: NumberLong("-9007199254740993"), text: "a.b", active: false,
     amount: NumberDecimal("-2.50"), observed: new Date(0), bytes: BinData(0, ""), nested: []},
    {seq: NumberLong("3"), n: null, text: null, active: null, amount: null, observed: null},
    {seq: NumberLong("4"), text: ["alpha", "abc"], n: [NumberLong("7")], active: [true]},
    {seq: NumberLong("5"), n: "20", text: "中文-é", active: true,
     amount: NumberDecimal("1.234"), observed: new Date(253402300800000), bytes: BinData(128, "AP+A"),
     nested: [{amount: NumberDecimal("2.00"), tags: ["first"]}, {amount: NumberDecimal("1.234")}]},
    {seq: NumberLong("6"), n: NumberInt(20), text: "line\nalpha", active: false,
     amount: NumberDecimal("1E-6176"), observed: new Date(253402300799999)},
    {seq: NumberLong("7"), n: NumberLong("7"), text: "a%_b", active: true},
    {seq: NumberLong("8"), n: NumberLong("8"), text: "alpha ", active: true},
    {seq: NumberLong("9"), n: NumberLong("9"), text: "ALPHA", active: false},
    {seq: NumberLong("10"), n: NumberLong("10"), text: "", active: false}
]);
// The valid predicate field must not hide a failure in another projected field under strict decoding.
m1.dirty.insertMany([{seq: NumberLong("1"), n: NumberLong("10")}, {seq: NumberLong("2"), n: "bad"}]);
m1.createCollection("collated", {collation: {locale: "en", strength: 2}});
m1.collated.insertMany([{seq: NumberLong("1"), text: "alpha"}, {seq: NumberLong("2"), text: "ALPHA"},
    {seq: NumberLong("3"), text: "alpha "}]);
m1.paths.insertMany([{seq: NumberLong("1"), a: {b: NumberLong("10")}, "literal.dot": "a.b"},
    {seq: NumberLong("2"), a: NumberLong("10"), "literal.dot": "value"}]);

// The same physical value is mapped at multiple precisions to expose rounding and overflow.
m1.precision_records.insertMany([
    {seq: NumberLong("1"), observed: new Date(-1), amount: NumberDecimal("123.4500")},
    {seq: NumberLong("2"), observed: new Date(0), amount: NumberDecimal("0E-6176")},
    {seq: NumberLong("3"), observed: new Date(100), amount: NumberDecimal("1E-9")},
    {seq: NumberLong("4"), observed: new Date(-100), amount: NumberDecimal("9999999999999999999999999999999999")},
    {seq: NumberLong("5"), observed: new Date(10), amount: NumberDecimal("1E-38")},
    {seq: NumberLong("6"), observed: new Date(1), amount: NumberDecimal("0.1234567890123456789012345678901234")},
    {seq: NumberLong("7"), observed: new Date(253402300799999), amount: NumberDecimal("1E+38")},
    {seq: NumberLong("8"), observed: new Date(253402300800000), amount: NumberDecimal("NaN")},
    {seq: NumberLong("9"), observed: new Date(-1000), amount: NumberDecimal("1E-6176")},
    // UTC instants straddle the Los Angeles spring DST transition without changing the mapping.
    {seq: NumberLong("10"), observed: new Date("2024-03-10T09:59:59.999Z"), amount: NumberDecimal("1.23")},
    {seq: NumberLong("11"), observed: new Date("2024-03-10T10:00:00.000Z"), amount: NumberDecimal("1.23")}
]);
