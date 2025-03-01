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


// add literal cast like cast(cast('{"a" : 1}' as json) as variant) also, for map, struct, array, json
suite("cast_literal_to_variant") {
     //JSON to VARIANT
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2001::db8::1 ", " key2 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": true, " key2 ": 123, " key3 ": "2001::db8::1 ", " key4 ": "192.168.1.1", " key5 ": "2001::db8::1 ", " key6 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": " text ", " key2 ": " text ", " key3 ": "2025-03-01 12:34:56", " key4 ": "192.168.1.1", " key5 ": 123, " key6 ": 123, " key7 ": " text ", " key8 ": "192.168.1.1", " key9 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": " text ", " key2 ": "192.168.1.1", " key3 ": "192.168.1.1", " key4 ": "2025-03-01 12:34:56", " key5 ": 123, " key6 ": "2025-03-01 12:34:56", " key7 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 456.78, " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": "2025-03-01 12:34:56", " key5 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": 123, " key2 ": 123, " key3 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "192.168.1.1", " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": 123, " key5 ": "2025-03-01 12:34:56", " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "192.168.1.1", " key2 ": true, " key3 ": " text ", " key4 ": "2025-03-01 12:34:56", " key5 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": true, " key2 ": true, " key3 ": "2001::db8::1 ", " key4 ": " text ", " key5 ": "2025-03-01 12:34:56", " key6 ": " text ", " key7 ": 456.78, " key8 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1", " key2 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2001::db8::1 ", " key2 ": 123, " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " text ", " key2 ": " text ", " key3 ": 456.78, " key4 ": " 2001: db8:: 1 ", " key5 ": 456.78, " key6 ": "2025-03-01 12:34:56", " key7 ": true, " key8 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "2025-03-01 12:34:56", " key2 ": 123, " key3 ": "192.168.1.1", " key4 ": "2025-03-01 12:34:56", " key5 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": true, " key2 ": "2001::db8::1 ", " key3 ": 123, " key4 ": 456.78, " key5 ": "2001::db8::1 ", " key6 ": 456.78, " key7 ": 456.78, " key8 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "2001::db8::1 ", " key2 ": 456.78, " key3 ": 123, " key4 ": "192.168.1.1", " key5 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "2001::db8::1 ", " key2 ": 123, " key3 ": "192.168.1.1", " key4 ": "192.168.1.1", " key5 ": "2025-03-01 12:34:56", " key6 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": " text ", " key2 ": "2025-03-01 12:34:56", " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": " text ", " key2 ": " 2001 : db8::1 ", " key3 ": 456.78, " key4 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "192.168.1.1", " key2 ": "192.168.1.1", " key3 ": " 2001: db8:: 1 ", " key4 ": 123, " key5 ": "2025-03-01 12:34:56", " key6 ": 123, " key7 ": 456.78, " key8 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 123, " key2 ": true, " key3 ": "192.168.1.1", " key4 ": "192.168.1.1", " key5 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": " text ", " key2 ": true, " key3 ": 456.78, " key4 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "192.168.1.1", " key2 ": "2025-03-01 12:34:56", " key3 ": "2001::db8::1 ", " key4 ": 456.78, " key5 ": "192.168.1.1", " key6 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2001::db8::1 ", " key2 ": "2025-03-01 12:34:56", " key3 ": " text ", " key4 ": "2025-03-01 12:34:56", " key5 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2025-03-01 12:34:56", " key2 ": true, " key3 ": 123, " key4 ": true, " key5 ": "2001::db8::1 ", " key6 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "2025-03-01 12:34:56", " key2 ": true, " key3 ": 123, " key4 ": "192.168.1.1", " key5 ": " text ", " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "192.168.1.1", " key2 ": "192.168.1.1", " key3 ": 456.78, " key4 ": true, " key5 ": "2025-03-01 12:34:56", " key6 ": true, " key7 ": 123, " key8 ": "2001::db8::1 ", " key9 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2001::db8::1 ", " key2 ": true, " key3 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 456.78, " key2 ": " text ", " key3 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 456.78, " key2 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "2001::db8::1 ", " key2 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "2001::db8::1 ", " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": 123, " key5 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "2001::db8::1 ", " key2 ": true, " key3 ": " text ", " key4 ": 456.78, " key5 ": "2025-03-01 12:34:56", " key6 ": " text ", " key7 ": 123, " key8 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2025-03-01 12:34:56", " key2 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "2001::db8::1 ", " key2 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123, " key2 ": "192.168.1.1", " key3 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": " text ", " key2 ": 456.78, " key3 ": "2025-03-01 12:34:56", " key4 ": " text ", " key5 ": "192.168.1.1", " key6 ": "2025-03-01 12:34:56", " key7 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": true, " key2 ": true, " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2001::db8::1 ", " key2 ": true, " key3 ": "2025-03-01 12:34:56", " key4 ": "2001::db8::1 ", " key5 ": 123, " key6 ": "192.168.1.1", " key7 ": true, " key8 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123, " key2 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "192.168.1.1", " key2 ": 456.78, " key3 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1", " key2 ": " text ", " key3 ": 123, " key4 ": "2025-03-01 12:34:56", " key5 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 123, " key2 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": 456.78, " key2 ": " text ", " key3 ": "2001::db8::1 ", " key4 ": true, " key5 ": "2001::db8::1 ", " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": 123, " key3 ": true, " key4 ": "192.168.1.1", " key5 ": "192.168.1.1", " key6 ": "2025-03-01 12:34:56", " key7 ": "2001::db8::1 ", " key8 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123, " key2 ": 456.78, " key3 ": "192.168.1.1", " key4 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": 123, " key2 ": " text ", " key3 ": "192.168.1.1", " key4 ": "2001::db8::1 ", " key5 ": true, " key6 ": true, " key7 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 456.78, " key2 ": 123, " key3 ": 456.78, " key4 ": " text ", " key5 ": 123, " key6 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "2025-03-01 12:34:56", " key2 ": "192.168.1.1", " key3 ": "2001::db8::1 ", " key4 ": "2001::db8::1 ", " key5 ": true, " key6 ": "2001::db8::1 ", " key7 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "2001::db8::1 ", " key2 ": true, " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2025-03-01 12:34:56", " key2 ": " text ", " key3 ": "2025-03-01 12:34:56", " key4 ": "192.168.1.1", " key5 ": 123, " key6 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": true, " key2 ": " 2001: db8:: 1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": true, " key2 ": " text ", " key3 ": "2025-03-01 12:34:56", " key4 ": true, " key5 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "192.168.1.1", " key2 ": " text ", " key3 ": true, " key4 ": true, " key5 ": " text ", " key6 ": true, " key7 ": "2001::db8::1 ", " key8 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": " text ", " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": " 2001: db8 ::1 ", " key5 ": " text ", " key6 ": true, " key7 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": true, " key2 ": "2001::db8::1 ", " key3 ": " text ", " key4 ": 123, " key5 ": "192.168.1.1", " key6 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 456.78, " key2 ": "192.168.1.1", " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": true, " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": 123, " key5 ": "192.168.1.1", " key6 ": " text ", " key7 ": 456.78, " key8 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": " text ", " key2 ": "192.168.1.1", " key3 ": "192.168.1.1", " key4 ": " text ", " key5 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "2025-03-01 12:34:56", " key2 ": " text ", " key3 ": "192.168.1.1", " key4 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "192.168.1.1", " key2 ": " 2001: db8:: 1 ", " key3 ": "2001::db8::1 ", " key4 ": true, " key5 ": "2001::db8::1 ", " key6 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": true, " key2 ": 456.78, " key3 ": true, " key4 ": "2025-03-01 12:34:56", " key5 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "192.168.1.1", " key2 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "192.168.1.1", " key2 ": 456.78, " key3 ": true, " key4 ": "2001::db8::1 ", " key5 ": 123, " key6 ": "2001::db8::1 ", " key7 ": " text ", " key8 ": " text ", " key9 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 ", " key2 ": "2025-03-01 12:34:56", " key3 ": "192.168.1.1", " key4 ": 456.78, " key5 ": 456.78, " key6 ": 456.78, " key7 ": 456.78, " key8 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2025-03-01 12:34:56", " key2 ": 456.78, " key3 ": 123, " key4 ": 456.78, " key5 ": 123, " key6 ": 456.78, " key7 ": "192.168.1.1", " key8 ": "192.168.1.1", " key9 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "192.168.1.1", " key2 ": " text ", " key3 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 ", " key2 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "2025-03-01 12:34:56", " key2 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1", " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": "2025-03-01 12:34:56", " key5 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": true, " key2 ": 123, " key3 ": "192.168.1.1", " key4 ": true, " key5 ": 456.78, " key6 ": 456.78, " key7 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123, " key2 ": " text ", " key3 ": 456.78, " key4 ": 456.78, " key5 ": " text ", " key6 ": "192.168.1.1", " key7 ": 123, " key8 ": "2025-03-01 12:34:56", " key9 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": true, " key2 ": "192.168.1.1", " key3 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 456.78, " key2 ": 456.78, " key3 ": true, " key4 ": " text ", " key5 ": "2001::db8::1 ", " key6 ": true, " key7 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123, " key2 ": "2001::db8::1 ", " key3 ": "192.168.1.1", " key4 ": true, " key5 ": " 2001: db8:: 1 ", " key6 ": true, " key7 ": " 2001: db8:: 1 ", " key8 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": true, " key2 ": "192.168.1.1", " key3 ": " 2001: db8:: 1 ", " key4 ": "2001::db8::1 ", " key5 ": 456.78, " key6 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": true, " key3 ": "192.168.1.1", " key4 ": 123, " key5 ": 456.78, " key6 ": "2025-03-01 12:34:56", " key7 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": true, " key2 ": 456.78, " key3 ": true, " key4 ": true, " key5 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "192.168.1.1", " key2 ": "2025-03-01 12:34:56", " key3 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": true, " key2 ": "2001::db8::1 ", " key3 ": "192.168.1.1", " key4 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2001::db8::1 ", " key2 ": true, " key3 ": true, " key4 ": 123, " key5 ": "192.168.1.1", " key6 ": true, " key7 ": " text ", " key8 ": 456.78, " key9 ": 456.78}' AS VARIANT); """

     //ARRAY to VARIANT
    qt_sql """ SELECT CAST('[123, "192.168.1.1", 456.78, true, " text ", " 2001: db8:: 1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", " text ", " 2001: db8:: 1 ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "192.168.1.1", true, true, "2025-03-01 12:34:56", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, 456.78, true, 456.78, true]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", "192.168.1.1", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, true, 456.78, "2001::db8::1 ", " text ", "2025-03-01 12:34:56", 456.78, 123, "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", " text ", "192.168.1.1", 456.78, 456.78, true, 123, "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", 456.78, "2001::db8::1 ", " text ", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, true, 456.78, 123, 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, 123, "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, " text ", " text ", true, "192.168.1.1", 456.78, " text ", 123, " text ", " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", "192.168.1.1", true, true]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", true, "192.168.1.1", "192.168.1.1", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", 123, "192.168.1.1", "2025-03-01 12:34:56", "2001::db8::1 ", 123, " text ", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", 456.78, 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", "192.168.1.1", "192.168.1.1", " text ", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, " text ", "192.168.1.1", true, "192.168.1.1", " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, 456.78, "2001::db8::1 ", "192.168.1.1", "2025-03-01 12:34:56", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, 456.78, 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "2025-03-01 12:34:56", "192.168.1.1", " text ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", 456.78, "2025-03-01 12:34:56", "2001::db8::1 ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, 456.78, "2025-03-01 12:34:56", 456.78, " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", "2025-03-01 12:34:56", "192.168.1.1", "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", 456.78, "2025-03-01 12:34:56", "2025-03-01 12:34:56", "192.168.1.1", "2025-03-01 12:34:56", 123, "2025-03-01 12:34:56", "2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, " text ", "192.168.1.1", "192.168.1.1", "2001::db8::1 ", " 2001: db8:: 1 ", "192.168.1.1", " text ", "2025-03-01 12:34:56", " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", 123, " text ", 123, "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, true, "2001::db8::1 ", " 2001: db8:: 1 ", 456.78, 123, "2025-03-01 12:34:56", " text ", "2001::db8::1 ", " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, true, 123, "2025-03-01 12:34:56", 123, "2025-03-01 12:34:56", 123, 123, 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", true, " text ", 456.78, 456.78, "2025-03-01 12:34:56", "192.168.1.1", 456.78, 456.78, true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, true, true, 456.78, true, " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, 456.78, "2001::db8::1 ", 123, " text ", true, "2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, "2001::db8::1 ", " 2001: db8:: 1 ", "2025-03-01 12:34:56", "2025-03-01 12:34:56", "2025-03-01 12:34:56", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, "192.168.1.1", 123, true, "2025-03-01 12:34:56", " text ", " text ", 456.78, "2001::db8::1 ", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", "192.168.1.1", " text ", true, 123, "2001::db8::1 ", " text ", "2025-03-01 12:34:56", "192.168.1.1", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", 456.78, 456.78, "2025-03-01 12:34:56", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, 123, "2025-03-01 12:34:56", "2025-03-01 12:34:56", "2025-03-01 12:34:56", "2001::db8::1 ", "2025-03-01 12:34:56", 456.78, 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "192.168.1.1", 123, "2001::db8::1 ", "2025-03-01 12:34:56", " text ", true, " text ", "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", "2001::db8::1 ", "192.168.1.1", "2025-03-01 12:34:56", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "2025-03-01 12:34:56", 123, "2001::db8::1 ", 456.78, "2025-03-01 12:34:56", "2025-03-01 12:34:56", " text ", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", "2025-03-01 12:34:56", "192.168.1.1", 123, 123, "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, 456.78, "192.168.1.1", "192.168.1.1", " text ", "2025-03-01 12:34:56", true, " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, 456.78, "192.168.1.1", " text ", " 2001: db8:: 1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", "2025-03-01 12:34:56", 456.78, true, true, true, true, 456.78, 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", 123, true, "192.168.1.1", " text ", "192.168.1.1", " text ", "2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, "2001::db8::1 ", " 2001: db8:: 1 ", true, "192.168.1.1", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", "2025-03-01 12:34:56", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, "2001::db8::1 ", " 2001: db8:: 1 ", "2025-03-01 12:34:56", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", "192.168.1.1", true, 456.78, 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, "2025-03-01 12:34:56", true, 123, "192.168.1.1", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", 123, true, "192.168.1.1", true, 456.78, " text ", " text ", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, true, 456.78, "2025-03-01 12:34:56", 123, true, "2025-03-01 12:34:56", 456.78, "2001::db8::1 ", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", 456.78, "2001::db8::1 ", " text ", 456.78, true, "2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", "2025-03-01 12:34:56", " text ", "2001::db8::1 ", " text ", 123, 456.78, "2001::db8::1 ", "2025-03-01 12:34:56", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, "192.168.1.1", "2001::db8::1 ", " text ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", 123, 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, 456.78, " text ", true, " text ", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", true, "2025-03-01 12:34:56", "2001::db8::1 ", true, " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", "2025-03-01 12:34:56", "192.168.1.1", true, " 2001 : db8::1 ", "192.168.1.1", " text ", 456.78, " text ", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, "2001::db8::1 ", " 2001: db8:: 1 ", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, 123, "192.168.1.1", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, true, "2025-03-01 12:34:56", " text ", "2001::db8::1 ", true, " 2001 : db8::1 ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "192.168.1.1", 456.78, "2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, " text ", 123, 123, "2001::db8::1 ", "2001::db8::1 ", " text ", "2025-03-01 12:34:56", "192.168.1.1", true]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", "192.168.1.1", true, "192.168.1.1", 456.78, "2001::db8::1 ", " text ", 123, 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", " text ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, 456.78, "192.168.1.1", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true]' AS VARIANT); """
    qt_sql """ SELECT CAST('[true, "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", "2025-03-01 12:34:56", true, "2025-03-01 12:34:56", " text ", " text ", true, "2025-03-01 12:34:56", "2025-03-01 12:34:56"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, true, "192.168.1.1", "192.168.1.1", 456.78, "2025-03-01 12:34:56", 123, "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", true, 123, "2025-03-01 12:34:56", 456.78, 123, "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", true, "192.168.1.1", " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", true, " text ", "2025-03-01 12:34:56", " 2001 : db8::1 ", "2025-03-01 12:34:56", "192.168.1.1", " text "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[123, "192.168.1.1", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2001::db8::1 ", 123, " 2001: db8:: 1 ", " text ", "192.168.1.1", 123, "2025-03-01 12:34:56", "2025-03-01 12:34:56", "2001::db8::1 ", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('[" text ", 123, " text ", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", " text ", " text ", "2025-03-01 12:34:56", 123]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", "2001::db8::1 ", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "2001::db8::1 ", "192.168.1.1", " text ", "2001::db8::1 ", " text ", 456.78]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", "192.168.1.1", " text ", true, true, " 2001: db8 ::1 ", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('[456.78, true, " text ", " text ", 456.78, true, "2025-03-01 12:34:56", "2001::db8::1 "]' AS VARIANT); """
    qt_sql """ SELECT CAST('["192.168.1.1", "2001::db8::1 ", "192.168.1.1"]' AS VARIANT); """
    qt_sql """ SELECT CAST('["2025-03-01 12:34:56", 456.78, "2001::db8::1 ", 123, "192.168.1.1", " text ", " text "]' AS VARIANT); """

     //MAP to VARIANT
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 456.78, " key2 ": "2025-03-01 12:34:56", " key3 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "192.168.1.1", " key2 ": "2001::db8::1 ", " key3 ": 456.78, " key4 ": "192.168.1.1", " key5 ": "2025-03-01 12:34:56", " key6 ": "2025-03-01 12:34:56", " key7 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": true, " key2 ": " text ", " key3 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 456.78, " key2 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": true, " key2 ": " text ", " key3 ": 456.78, " key4 ": 456.78, " key5 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": true, " key2 ": "2025-03-01 12:34:56", " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "192.168.1.1", " key2 ": 456.78, " key3 ": 456.78, " key4 ": "2025-03-01 12:34:56", " key5 ": " text ", " key6 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 123, " key2 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "2001::db8::1 ", " key2 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": true, " key2 ": " 2001: db8:: 1 ", " key3 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1", " key2 ": 123, " key3 ": 456.78, " key4 ": "192.168.1.1", " key5 ": "192.168.1.1", " key6 ": " 2001: db8:: 1 ", " key7 ": "192.168.1.1", " key8 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 123, " key2 ": " text ", " key3 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": true, " key2 ": 456.78, " key3 ": 123, " key4 ": true, " key5 ": " 2001: db8:: 1 ", " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " text ", " key2 ": true, " key3 ": "2025-03-01 12:34:56", " key4 ": true, " key5 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": " text ", " key2 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": 456.78, " key3 ": " text ", " key4 ": 123, " key5 ": 456.78, " key6 ": "192.168.1.1", " key7 ": "2025-03-01 12:34:56", " key8 ": " text ", " key9 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 456.78, " key2 ": " text ", " key3 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 123, " key2 ": "2025-03-01 12:34:56", " key3 ": "2025-03-01 12:34:56", " key4 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": true, " key2 ": "192.168.1.1", " key3 ": 123, " key4 ": true, " key5 ": 123, " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "192.168.1.1", " key2 ": 456.78, " key3 ": "2001::db8::1 ", " key4 ": "192.168.1.1", " key5 ": 123, " key6 ": 456.78, " key7 ": "2025-03-01 12:34:56", " key8 ": "2025-03-01 12:34:56", " key9 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": true, " key2 ": "192.168.1.1", " key3 ": 456.78, " key4 ": " text ", " key5 ": "192.168.1.1", " key6 ": "192.168.1.1", " key7 ": "192.168.1.1", " key8 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": true, " key2 ": true, " key3 ": " text ", " key4 ": true, " key5 ": 456.78, " key6 ": " 2001: db8:: 1 ", " key7 ": "2001::db8::1 ", " key8 ": " text ", " key9 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 123, " key2 ": " 2001: db8:: 1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": "2025-03-01 12:34:56", " key3 ": "192.168.1.1", " key4 ": " text ", " key5 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": " text ", " key2 ": 456.78, " key3 ": true, " key4 ": "192.168.1.1", " key5 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "2025-03-01 12:34:56", " key2 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2025-03-01 12:34:56", " key2 ": 123, " key3 ": 123, " key4 ": 456.78, " key5 ": 123, " key6 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 456.78, " key2 ": 456.78, " key3 ": 123, " key4 ": 456.78, " key5 ": "2025-03-01 12:34:56", " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 123, " key2 ": 456.78, " key3 ": "2001::db8::1 ", " key4 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": true, " key2 ": 456.78, " key3 ": "192.168.1.1", " key4 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": " text ", " key2 ": 123, " key3 ": "2025-03-01 12:34:56", " key4 ": " text ", " key5 ": "192.168.1.1", " key6 ": "2025-03-01 12:34:56", " key7 ": 123, " key8 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": true, " key2 ": 123, " key3 ": true, " key4 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": 123, " key2 ": "192.168.1.1", " key3 ": "2001::db8::1 ", " key4 ": "2025-03-01 12:34:56", " key5 ": "2025-03-01 12:34:56", " key6 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": true, " key2 ": "192.168.1.1", " key3 ": "2001::db8::1 ", " key4 ": "2025-03-01 12:34:56", " key5 ": "192.168.1.1", " key6 ": 123, " key7 ": 456.78, " key8 ": "192.168.1.1", " key9 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "192.168.1.1", " key2 ": true, " key3 ": "2025-03-01 12:34:56", " key4 ": true, " key5 ": true, " key6 ": 456.78, " key7 ": "192.168.1.1", " key8 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": true, " key2 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": true, " key2 ": 456.78, " key3 ": "2001::db8::1 ", " key4 ": true, " key5 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1", " key2 ": true, " key3 ": "192.168.1.1", " key4 ": 456.78, " key5 ": "2001::db8::1 ", " key6 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": " text ", " key2 ": "192.168.1.1", " key3 ": 123, " key4 ": "192.168.1.1", " key5 ": " text ", " key6 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 ", " key2 ": "192.168.1.1", " key3 ": true, " key4 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " text ", " key2 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": 123, " key2 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 123, " key2 ": "192.168.1.1", " key3 ": 456.78, " key4 ": "192.168.1.1", " key5 ": "192.168.1.1", " key6 ": 123, " key7 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": "192.168.1.1", " key2 ": "2025-03-01 12:34:56", " key3 ": "2025-03-01 12:34:56", " key4 ": true, " key5 ": " 2001 : db8::1 ", " key6 ": "2025-03-01 12:34:56", " key7 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": " text ", " key2 ": 123, " key3 ": true, " key4 ": "2025-03-01 12:34:56", " key5 ": "2001::db8::1 ", " key6 ": " text ", " key7 ": 456.78, " key8 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": 123, " key3 ": "192.168.1.1", " key4 ": " text ", " key5 ": " 2001: db8:: 1 ", " key6 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "2025-03-01 12:34:56", " key2 ": "2025-03-01 12:34:56", " key3 ": 123, " key4 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": 456.78, " key2 ": " text ", " key3 ": "2001::db8::1 ", " key4 ": 123, " key5 ": "2001::db8::1 ", " key6 ": 456.78, " key7 ": "2001::db8::1 ", " key8 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": "2025-03-01 12:34:56", " key2 ": 456.78, " key3 ": true, " key4 ": " text ", " key5 ": " text ", " key6 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": " text ", " key2 ": "192.168.1.1", " key3 ": 456.78, " key4 ": true, " key5 ": "192.168.1.1", " key6 ": "2001::db8::1 ", " key7 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": 456.78, " key2 ": "192.168.1.1", " key3 ": "2025-03-01 12:34:56", " key4 ": "192.168.1.1", " key5 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "192.168.1.1", " key2 ": true, " key3 ": " text ", " key4 ": " 2001: db8:: 1 ", " key5 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": true, " key3 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 456.78, " key2 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": " text ", " key2 ": true, " key3 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 123, " key2 ": " text ", " key3 ": "192.168.1.1", " key4 ": "2001::db8::1 ", " key5 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1", " key2 ": 456.78, " key3 ": 123, " key4 ": 123, " key5 ": " text ", " key6 ": "192.168.1.1", " key7 ": "2025-03-01 12:34:56", " key8 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": "2025-03-01 12:34:56", " key2 ": "2001::db8::1 ", " key3 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " text ", " key2 ": 123, " key3 ": "2001::db8::1 ", " key4 ": 123, " key5 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": 456.78, " key2 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 456.78, " key2 ": true, " key3 ": "2025-03-01 12:34:56", " key4 ": "192.168.1.1", " key5 ": "192.168.1.1", " key6 ": "2025-03-01 12:34:56", " key7 ": "2025-03-01 12:34:56", " key8 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 ", " key2 ": "192.168.1.1", " key3 ": "192.168.1.1", " key4 ": true, " key5 ": "2025-03-01 12:34:56", " key6 ": " text ", " key7 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 123, " key1 ": 456.78, " key2 ": 123, " key3 ": 123, " key4 ": true, " key5 ": 123, " key6 ": 456.78, " key7 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 ", " key2 ": 123, " key3 ": " text ", " key4 ": true, " key5 ": 123, " key6 ": true, " key7 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": "2001::db8::1 ", " key2 ": 123, " key3 ": "192.168.1.1", " key4 ": "192.168.1.1", " key5 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": " text ", " key2 ": true, " key3 ": "2001::db8::1 ", " key4 ": "192.168.1.1", " key5 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": "192.168.1.1", " key2 ": 456.78, " key3 ": "2025-03-01 12:34:56", " key4 ": "192.168.1.1", " key5 ": "192.168.1.1", " key6 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2025-03-01 12:34:56", " key2 ": " text ", " key3 ": "2025-03-01 12:34:56", " key4 ": "192.168.1.1", " key5 ": "2001::db8::1 ", " key6 ": "192.168.1.1", " key7 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": true, " key2 ": 123, " key3 ": " 2001 : db8::1 ", " key4 ": 123, " key5 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": " text ", " key2 ": " text ", " key3 ": "192.168.1.1", " key4 ": "2025-03-01 12:34:56", " key5 ": "2025-03-01 12:34:56", " key6 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2001::db8::1 ", " key1 ": 456.78, " key2 ": 456.78, " key3 ": true, " key4 ": 456.78, " key5 ": 456.78, " key6 ": 456.78, " key7 ": 123, " key8 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "2001::db8::1 ", " key2 ": "192.168.1.1", " key3 ": 123, " key4 ": true}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": 123, " key2 ": "2025-03-01 12:34:56", " key3 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": " text ", " key1 ": " 2001: db8:: 1 ", " key2 ": "192.168.1.1", " key3 ": "2025-03-01 12:34:56"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": 456.78, " key1 ": true, " key2 ": "2025-03-01 12:34:56", " key3 ": "192.168.1.1", " key4 ": 456.78, " key5 ": "2025-03-01 12:34:56", " key6 ": "192.168.1.1", " key7 ": "2025-03-01 12:34:56", " key8 ": 456.78, " key9 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": 456.78, " key2 ": " text ", " key3 ": "2001::db8::1 "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": true, " key2 ": 123, " key3 ": 123}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": true, " key1 ": "2001::db8::1 ", " key2 ": 456.78}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": " text ", " key2 ": true, " key3 ": 456.78, " key4 ": true, " key5 ": "192.168.1.1", " key6 ": 123, " key7 ": 456.78, " key8 ": "192.168.1.1"}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "2025-03-01 12:34:56", " key1 ": " text ", " key2 ": " 2001 : db8::1 ", " key3 ": "2001::db8::1 ", " key4 ": " text "}' AS VARIANT); """
    qt_sql """ SELECT CAST('{" key0 ": "192.168.1.1", " key1 ": "192.168.1.1", " key2 ": "2001::db8::1 ", " key3 ": true, " key4 ": 456.78, " key5 ": "192.168.1.1", " key6 ": true}' AS VARIANT); """

     //STRUCT to VARIANT
    qt_sql """ SELECT CAST(named_struct ( "f0", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", 123, "f2", 456.78, "f3", "2001:db8::1", "f4", 456.78, "f5", "text", "f6", "2025-03-01 12:34:56", "f7", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "192.168.1.1", "f2", true, "f3", 456.78, "f4", "2025-03-01 12:34:56", "f5", "2001:db8::1", "f6", 123, "f7", "text", "f8", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "text", "f2", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "192.168.1.1", "f2", "text", "f3", "2025-03-01 12:34:56", "f4", 456.78, "f5", "192.168.1.1", "f6", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", "text", "f2", "text", "f3", "2025-03-01 12:34:56", "f4", "2001:db8::1", "f5", 123, "f6", "2025-03-01 12:34:56", "f7", 123, "f8", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "text", "f2", "2001:db8::1", "f3", "2001:db8::1", "f4", 123, "f5", true, "f6", 456.78, "f7", 456.78, "f8", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", 123, "f2", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", 456.78, "f2", "2025-03-01 12:34:56", "f3", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "text", "f2", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "2001:db8::1", "f2", true, "f3", "192.168.1.1", "f4", true, "f5", 456.78, "f6", "text", "f7", 123, "f8", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", "text", "f2", "2025-03-01 12:34:56", "f3", true, "f4", "2025-03-01 12:34:56", "f5", "2025-03-01 12:34:56", "f6", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "text", "f2", true, "f3", 456.78, "f4", 456.78, "f5", "2001:db8::1", "f6", "192.168.1.1", "f7", "2001:db8::1", "f8", "2025-03-01 12:34:56", "f9", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "text", "f2", true, "f3", "text", "f4", "text", "f5", "2025-03-01 12:34:56", "f6", "text", "f7", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", 123, "f2", "2025-03-01 12:34:56", "f3", "text", "f4", true, "f5", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "192.168.1.1", "f2", true, "f3", "text", "f4", 123)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", true, "f2", "text", "f3", "192.168.1.1", "f4", "2001:db8::1", "f5", "192.168.1.1", "f6", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "2025-03-01 12:34:56", "f2", true, "f3", 123)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", 123, "f2", "192.168.1.1", "f3", "2001:db8::1", "f4", true, "f5", 456.78, "f6", true, "f7", 123, "f8", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "2001:db8::1", "f2", 123, "f3", 123, "f4", "2001:db8::1", "f5", "text", "f6", 456.78, "f7", 456.78, "f8", 123, "f9", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "2001:db8::1", "f2", 456.78, "f3", "192.168.1.1", "f4", 123, "f5", "2001:db8::1", "f6", "2025-03-01 12:34:56", "f7", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", 456.78, "f2", "192.168.1.1", "f3", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "192.168.1.1", "f2", "192.168.1.1", "f3", "text", "f4", "2001:db8::1", "f5", "2001:db8::1", "f6", 123, "f7", true, "f8", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "192.168.1.1", "f2", "192.168.1.1", "f3", true, "f4", "192.168.1.1", "f5", true, "f6", true, "f7", 123)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", 456.78, "f2", "192.168.1.1", "f3", 123, "f4", 123, "f5", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", true, "f2", 456.78, "f3", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "text", "f2", 123, "f3", 456.78, "f4", "2025-03-01 12:34:56", "f5", "text", "f6", true, "f7", true, "f8", 123)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "2025-03-01 12:34:56", "f2", "192.168.1.1", "f3", true, "f4", "2001:db8::1", "f5", true, "f6", "text", "f7", 456.78, "f8", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", 456.78, "f2", 123, "f3", "text", "f4", 123, "f5", "192.168.1.1", "f6", 123, "f7", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "text", "f2", "2001:db8::1", "f3", true, "f4", "2025-03-01 12:34:56", "f5", true, "f6", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", true, "f2", 456.78, "f3", "2001:db8::1", "f4", "text", "f5", true, "f6", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "2001:db8::1", "f2", 456.78, "f3", 456.78, "f4", 456.78, "f5", 456.78, "f6", 456.78, "f7", "2001:db8::1", "f8", "text", "f9", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", 123, "f2", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", 456.78, "f2", "2025-03-01 12:34:56", "f3", 123, "f4", "2025-03-01 12:34:56", "f5", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", "192.168.1.1", "f2", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", true, "f2", "2001:db8::1", "f3", "2001:db8::1", "f4", "2025-03-01 12:34:56", "f5", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "2001:db8::1", "f2", true, "f3", true, "f4", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "text", "f2", "text", "f3", "192.168.1.1", "f4", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", "text", "f2", "2025-03-01 12:34:56", "f3", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "2001:db8::1", "f2", "2025-03-01 12:34:56", "f3", "2025-03-01 12:34:56", "f4", "2025-03-01 12:34:56", "f5", "2025-03-01 12:34:56", "f6", "2025-03-01 12:34:56", "f7", "192.168.1.1", "f8", "192.168.1.1", "f9", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "text", "f2", "2025-03-01 12:34:56", "f3", "192.168.1.1", "f4", 456.78, "f5", 456.78, "f6", "2001:db8::1", "f7", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", 456.78, "f2", 456.78, "f3", 456.78, "f4", true, "f5", 456.78, "f6", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "text", "f2", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", true, "f2", true, "f3", 123, "f4", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", 123, "f2", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "2025-03-01 12:34:56", "f2", "192.168.1.1", "f3", "2001:db8::1", "f4", "2001:db8::1", "f5", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "text", "f2", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", "text", "f2", true, "f3", true, "f4", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", 123, "f2", "text", "f3", "2025-03-01 12:34:56", "f4", 123, "f5", true, "f6", true, "f7", true, "f8", 456.78, "f9", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", "2001:db8::1", "f2", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "text", "f2", "text", "f3", "2025-03-01 12:34:56", "f4", "text", "f5", "2025-03-01 12:34:56", "f6", "2025-03-01 12:34:56", "f7", "2001:db8::1", "f8", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", 456.78, "f2", "2025-03-01 12:34:56", "f3", true, "f4", true, "f5", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", 456.78, "f2", 123, "f3", true, "f4", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "2025-03-01 12:34:56", "f2", "192.168.1.1", "f3", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", "192.168.1.1", "f2", "text", "f3", 456.78, "f4", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", 123, "f2", "text", "f3", "2001:db8::1", "f4", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "192.168.1.1", "f2", "192.168.1.1", "f3", "2001:db8::1", "f4", 123, "f5", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", true, "f2", "text", "f3", "2025-03-01 12:34:56", "f4", "text", "f5", "text", "f6", "2025-03-01 12:34:56", "f7", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", "2001:db8::1", "f2", "2001:db8::1", "f3", "text", "f4", "192.168.1.1", "f5", "text", "f6", true, "f7", "text", "f8", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", 456.78, "f2", 123, "f3", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", 123, "f2", "2025-03-01 12:34:56", "f3", "text", "f4", true, "f5", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", true, "f2", true, "f3", true)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", 123, "f2", "text", "f3", 456.78, "f4", 123, "f5", 123, "f6", "2001:db8::1", "f7", "text", "f8", "192.168.1.1", "f9", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "192.168.1.1", "f1", 456.78, "f2", 456.78, "f3", "text", "f4", "192.168.1.1", "f5", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", "text", "f2", "2025-03-01 12:34:56", "f3", 456.78, "f4", "2025-03-01 12:34:56", "f5", 456.78, "f6", 123, "f7", "192.168.1.1", "f8", "2025-03-01 12:34:56", "f9", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 123, "f1", "2001:db8::1", "f2", "192.168.1.1", "f3", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", "2001:db8::1", "f2", "192.168.1.1", "f3", "192.168.1.1", "f4", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", "2001:db8::1", "f2", true, "f3", "192.168.1.1", "f4", 456.78, "f5", "2025-03-01 12:34:56", "f6", "192.168.1.1", "f7", "text", "f8", "2001:db8::1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "2001:db8::1", "f2", "2025-03-01 12:34:56", "f3", "192.168.1.1", "f4", true, "f5", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", 123, "f2", true, "f3", 456.78, "f4", "192.168.1.1", "f5", "192.168.1.1", "f6", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", true, "f2", "192.168.1.1", "f3", 456.78)
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2025-03-01 12:34:56", "f1", "2001:db8::1", "f2", "2025-03-01 12:34:56", "f3", true, "f4", "2001:db8::1", "f5", "text", "f6", "2025-03-01 12:34:56", "f7", "text", "f8", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", true, "f1", true, "f2", "2025-03-01 12:34:56", "f3", "2025-03-01 12:34:56")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", 456.78, "f1", "2001:db8::1", "f2", "2001:db8::1", "f3", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "text", "f1", "2025-03-01 12:34:56", "f2", 123, "f3", "text", "f4", "192.168.1.1")
    AS VARIANT ); """
    qt_sql """ SELECT CAST(named_struct ( "f0", "2001:db8::1", "f1", true, "f2", "2001:db8::1", "f3", 123, "f4", 456.78, "f5", "2001:db8::1", "f6", "2025-03-01 12:34:56", "f7", "2025-03-01 12:34:56")
    AS VARIANT ); """
}
