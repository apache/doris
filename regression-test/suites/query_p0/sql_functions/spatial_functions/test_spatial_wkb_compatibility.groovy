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

suite("test_spatial_wkb_compatibility") {
    // Legacy STRING functions keep their historical XY interpretation of EWKB dimension flags.
    // The new typed functions continue to reject Z/M and embedded SRID metadata.
    ["0101000080", "0101000040", "01010000C0", "01010000A0E6100000",
     "0101000060E6100000", "01010000E0E6100000"].eachWithIndex { header, index ->
        def ewkb = header + "000000000000F03F0000000000000040" + "00000000000008400000000000001040"
        "qt_ewkb_${index}"("""select ST_AsText(ST_GeometryFromWKB('${ewkb}')),
            ST_AsText(ST_GeomFromWKB('${ewkb}')),
            ST_GeometryFromWKBTyped('${ewkb}') is null,
            ST_GeogFromWKB('${ewkb}') is null""")
    }
    // ISO WKB dimensional type codes were never part of the legacy XY contract.
    qt_iso_dimensions """select ST_GeometryFromWKB(
        '01E9030000000000000000F03F00000000000000400000000000000840') is null,
        ST_GeometryFromWKBTyped(
        '01E9030000000000000000F03F00000000000000400000000000000840') is null"""
}
