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

suite("test_st_3d_dimension_functions", "arrow_flight_sql") {
    sql "set batch_size = 4096;"

    // ====================================================================
    // Section 1: ST_NDims — dimension accessor for all geometry types
    // ====================================================================

    // --- 1.1  2D geometries → ST_NDims = 2 ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT(1 2)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('LINESTRING(0 0, 1 1)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)))'));"
    qt_sql "SELECT ST_NDims(ST_Point(1, 2));"
    qt_sql "SELECT ST_NDims(ST_Circle(0, 0, 1));"

    // --- 1.2  3D Z geometries → ST_NDims = 3 ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'));"

    // --- 1.3  3D M geometries → ST_NDims = 3 ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT M(1 2 4)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('LINESTRING M(0 0 1, 1 1 2)'));"

    // --- 1.4  4D ZM geometries → ST_NDims = 4 ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT ZM(1 2 3 4)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('LINESTRING ZM(0 0 1 10, 1 1 2 20)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POLYGON ZM((0 0 0 0, 10 0 0 0, 10 10 0 0, 0 10 0 0, 0 0 0 0))'));"

    // --- 1.5  ST_NDims with NULL ---
    qt_sql "SELECT ST_NDims(NULL);"

    // ====================================================================
    // Section 2: ST_ZmFlag — ZM flag bitmask
    //   0 = no Z/M (2D), 1 = M only, 2 = Z only, 3 = Z + M
    // ====================================================================

    // --- 2.1  2D → flag = 0 ---
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POINT(1 2)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('LINESTRING(0 0, 1 1)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"
    qt_sql "SELECT ST_ZmFlag(ST_Point(1, 2));"
    qt_sql "SELECT ST_ZmFlag(ST_Circle(0, 0, 1));"

    // --- 2.2  Z only → flag = 2 ---
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'));"

    // --- 2.3  M only → flag = 1 ---
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POINT M(1 2 4)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('LINESTRING M(0 0 1, 1 1 2)'));"

    // --- 2.4  ZM → flag = 3 ---
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POINT ZM(1 2 3 4)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('LINESTRING ZM(0 0 1 10, 1 1 2 20)'));"

    // --- 2.5  ST_ZmFlag with NULL ---
    qt_sql "SELECT ST_ZmFlag(NULL);"

    // ====================================================================
    // Section 3: ST_Z — extract Z coordinate from a point
    // ====================================================================

    // --- 3.1  POINT Z → returns Z value ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(0 0 0)'));"
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(-1.5 2.5 -99.9)'));"
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(10.123456 -20.654321 300000.789)'));"

    // --- 3.2  POINT ZM → returns Z value (ignores M) ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT ZM(1 2 3 4)'));"

    // --- 3.3  2D POINT → returns NULL ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT(1 2)'));"
    qt_sql "SELECT ST_Z(ST_Point(1, 2));"

    // --- 3.4  M-only POINT → returns NULL (no Z) ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT M(1 2 4)'));"

    // --- 3.5  Non-point geometry → returns NULL ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('LINESTRING(0 0, 1 1)'));"
    qt_sql "SELECT ST_Z(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"

    // --- 3.6  ST_Z with NULL ---
    qt_sql "SELECT ST_Z(NULL);"

    // ====================================================================
    // Section 4: ST_M — extract M coordinate from a point
    // ====================================================================

    // --- 4.1  POINT M → returns M value ---
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT M(1 2 4)'));"
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT M(0 0 0)'));"
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT M(-1.5 2.5 -99.9)'));"

    // --- 4.2  POINT ZM → returns M value (ignores Z) ---
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT ZM(1 2 3 4)'));"

    // --- 4.3  2D POINT → returns NULL ---
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT(1 2)'));"
    qt_sql "SELECT ST_M(ST_Point(1, 2));"

    // --- 4.4  Z-only POINT → returns NULL (no M) ---
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT Z(1 2 3)'));"

    // --- 4.5  Non-point geometry → returns NULL ---
    qt_sql "SELECT ST_M(ST_GeomFromText('LINESTRING(0 0, 1 1)'));"
    qt_sql "SELECT ST_M(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"

    // --- 4.6  ST_M with NULL ---
    qt_sql "SELECT ST_M(NULL);"

    // ====================================================================
    // Section 5: ST_AsEWKB — Extended WKB output
    // ====================================================================

    // --- 5.1  2D geometries → standard WKB (no Z/M flags) ---
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('POINT(1 2)'));"
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('LINESTRING(0 0, 1 1)'));"
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"

    // --- 5.2  Z geometries → EWKB with Z flag (type |= 0x80000000) ---
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'));"

    // --- 5.3  M geometries → EWKB with M flag (type |= 0x40000000) ---
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('POINT M(1 2 4)'));"

    // --- 5.4  ZM geometries → EWKB with Z+M flags (type |= 0xC0000000) ---
    qt_sql "SELECT ST_AsEWKB(ST_GeomFromText('POINT ZM(1 2 3 4)'));"

    // --- 5.5  ST_AsEWKB with NULL ---
    qt_sql "SELECT ST_AsEWKB(NULL);"

    // ====================================================================
    // Section 6: WKT 3D parsing — round-trip via ST_AsText
    // ====================================================================

    // --- 6.1  POINT Z round-trip ---
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POINT Z(0 0 0)'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POINT Z(-180.0 90.0 -11.0)'));"

    // --- 6.2  POINT M round-trip ---
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POINT M(1 2 4)'));"

    // --- 6.3  POINT ZM round-trip ---
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POINT ZM(1 2 3 4)'));"

    // --- 6.4  LINESTRING Z round-trip ---
    qt_sql "SELECT ST_AsText(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2, 2 2 3)'));"

    // --- 6.5  POLYGON Z round-trip ---
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'));"

    // ====================================================================
    // Section 7: WKB/EWKB round-trip — parse EWKB then extract dimensions
    // ====================================================================

    // --- 7.1  EWKB(2D) → re-parse → ST_NDims / ST_AsText ---
    qt_sql "SELECT ST_NDims(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT(1 2)'))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT(1 2)'))));"

    // --- 7.2  EWKB(Z) → re-parse → ST_NDims / ST_Z / ST_AsText ---
    qt_sql "SELECT ST_NDims(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT Z(1 2 3)'))));"
    qt_sql "SELECT ST_Z(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT Z(1 2 3)'))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT Z(1 2 3)'))));"

    // --- 7.3  EWKB(M) → re-parse → ST_NDims / ST_M / ST_AsText ---
    qt_sql "SELECT ST_NDims(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT M(1 2 4)'))));"
    qt_sql "SELECT ST_M(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT M(1 2 4)'))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT M(1 2 4)'))));"

    // --- 7.4  EWKB(ZM) → re-parse → ST_NDims / ST_Z / ST_M / ST_AsText ---
    qt_sql "SELECT ST_NDims(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT ZM(1 2 3 4)'))));"
    qt_sql "SELECT ST_Z(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT ZM(1 2 3 4)'))));"
    qt_sql "SELECT ST_M(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT ZM(1 2 3 4)'))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POINT ZM(1 2 3 4)'))));"

    // --- 7.5  EWKB LINESTRING Z round-trip ---
    qt_sql "SELECT ST_NDims(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'))));"

    // --- 7.6  EWKB POLYGON Z round-trip ---
    qt_sql "SELECT ST_NDims(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'))));"

    // ====================================================================
    // Section 8: Backward compatibility — existing 2D functions on 3D input
    //   PostGIS semantic: 2D predicates/measures ignore Z, use XY only
    // ====================================================================

    // --- 8.1  ST_X / ST_Y still work on 2D points ---
    qt_sql "SELECT ST_X(ST_Point(1, 2));"
    qt_sql "SELECT ST_Y(ST_Point(1, 2));"
    qt_sql "SELECT ST_X(ST_GeomFromText('POINT(24.7 56.7)'));"
    qt_sql "SELECT ST_Y(ST_GeomFromText('POINT(24.7 56.7)'));"

    // --- 8.2  ST_X / ST_Y on 3D points — should still return X/Y ---
    qt_sql "SELECT ST_X(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_Y(ST_GeomFromText('POINT Z(1 2 3)'));"
    qt_sql "SELECT ST_X(ST_GeomFromText('POINT ZM(10 20 30 40)'));"
    qt_sql "SELECT ST_Y(ST_GeomFromText('POINT ZM(10 20 30 40)'));"

    // --- 8.3  ST_AsText on 2D still works as before ---
    qt_sql "SELECT ST_AsText(ST_Point(24.7, 56.7));"
    qt_sql "SELECT ST_AsText(ST_Circle(111, 64, 10000));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('LINESTRING(0 0, 1 0, 1 1, 0 1)'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"

    // --- 8.4  ST_Contains with 2D geometries unchanged ---
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Point(5, 5));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Point(50, 50));"

    // --- 8.5  ST_Distance with 2D geometries unchanged (geodesic, meters) ---
    qt_sql "SELECT ST_Distance(ST_Point(0, 0), ST_Point(0, 0));"

    // --- 8.6  ST_Length with 2D geometries unchanged ---
    qt_sql "SELECT ST_Length(ST_Point(0, 0));"

    // --- 8.7  ST_Area with 2D geometries unchanged ---
    qt_sql "SELECT ST_Area_Square_Meters(ST_Polygon(\"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))\"));"
    qt_sql "SELECT ST_Area_Square_Km(ST_Circle(0, 0, 1));"

    // --- 8.8  ST_AsBinary (standard WKB) on 2D unchanged ---
    qt_sql "SELECT ST_AsBinary(ST_GeomFromText('POINT(1 2)'));"

    // --- 8.9  ST_GeometryType still works ---
    qt_sql "SELECT ST_GeometryType(ST_Point(0, 0));"
    qt_sql "SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1)'));"
    qt_sql "SELECT ST_GeometryType(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));"

    // ====================================================================
    // Section 9: Table-driven tests — insert 3D data, query back
    // ====================================================================
    sql "DROP TABLE IF EXISTS geo_3d_test;"
    sql """
        CREATE TABLE geo_3d_test (
            id INT,
            geo VARCHAR(1000)
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    // --- 9.1  Insert 2D, Z, M, ZM points ---
    sql "INSERT INTO geo_3d_test VALUES (1, 'POINT(1 2)');"
    sql "INSERT INTO geo_3d_test VALUES (2, 'POINT Z(1 2 3)');"
    sql "INSERT INTO geo_3d_test VALUES (3, 'POINT M(1 2 4)');"
    sql "INSERT INTO geo_3d_test VALUES (4, 'POINT ZM(1 2 3 4)');"
    sql "INSERT INTO geo_3d_test VALUES (5, 'LINESTRING Z(0 0 1, 1 1 2, 2 2 3)');"
    sql "INSERT INTO geo_3d_test VALUES (6, 'POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))');"
    sql "INSERT INTO geo_3d_test VALUES (7, NULL);"

    // --- 9.2  Query ST_NDims from table (ordered by id) ---
    order_qt_sql "SELECT id, ST_NDims(ST_GeomFromText(geo)) FROM geo_3d_test ORDER BY id;"

    // --- 9.3  Query ST_ZmFlag from table ---
    order_qt_sql "SELECT id, ST_ZmFlag(ST_GeomFromText(geo)) FROM geo_3d_test ORDER BY id;"

    // --- 9.4  Query ST_Z from table (only points with Z return non-NULL) ---
    order_qt_sql "SELECT id, ST_Z(ST_GeomFromText(geo)) FROM geo_3d_test ORDER BY id;"

    // --- 9.5  Query ST_M from table (only points with M return non-NULL) ---
    order_qt_sql "SELECT id, ST_M(ST_GeomFromText(geo)) FROM geo_3d_test ORDER BY id;"

    // --- 9.6  Query ST_AsText from table ---
    order_qt_sql "SELECT id, ST_AsText(ST_GeomFromText(geo)) FROM geo_3d_test ORDER BY id;"

    // --- 9.7  Query ST_AsEWKB from table ---
    order_qt_sql "SELECT id, ST_AsEWKB(ST_GeomFromText(geo)) FROM geo_3d_test ORDER BY id;"

    // --- 9.8  EWKB round-trip from table ---
    order_qt_sql "SELECT id, ST_AsText(ST_GeomFromWKB(ST_AsEWKB(ST_GeomFromText(geo)))) FROM geo_3d_test WHERE geo IS NOT NULL ORDER BY id;"

    // --- 9.9  Filter by dimension ---
    order_qt_sql "SELECT id FROM geo_3d_test WHERE ST_NDims(ST_GeomFromText(geo)) = 2 ORDER BY id;"
    order_qt_sql "SELECT id FROM geo_3d_test WHERE ST_NDims(ST_GeomFromText(geo)) = 3 ORDER BY id;"
    order_qt_sql "SELECT id FROM geo_3d_test WHERE ST_NDims(ST_GeomFromText(geo)) = 4 ORDER BY id;"

    // --- 9.10  Use ST_Z in arithmetic ---
    order_qt_sql "SELECT id, ST_Z(ST_GeomFromText(geo)) + 100 FROM geo_3d_test WHERE ST_Z(ST_GeomFromText(geo)) IS NOT NULL ORDER BY id;"

    // ====================================================================
    // Section 10: Edge cases and special values
    // ====================================================================

    // --- 10.1  Zero coordinates ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT Z(0 0 0)'));"
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(0 0 0)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POINT Z(0 0 0)'));"

    // --- 10.2  Negative coordinates ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(-180 -90 -1000)'));"
    qt_sql "SELECT ST_M(ST_GeomFromText('POINT M(-180 -90 -1000)'));"

    // --- 10.3  Very large Z coordinate (x/y must be valid lon/lat) ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(1 1 10000)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT Z(0 0 9999)'));"

    // --- 10.4  Fractional coordinates ---
    qt_sql "SELECT ST_Z(ST_GeomFromText('POINT Z(1.123456789 2.987654321 3.14159265358979)'));"

    // --- 10.5  ST_AsEWKB consistency: 2D EWKB should equal standard WKB ---
    qt_sql "SELECT ST_AsBinary(ST_GeomFromText('POINT(1 2)')) = ST_AsEWKB(ST_GeomFromText('POINT(1 2)'));"

    // --- 10.6  Multiple dimension accessors on same geometry ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POINT ZM(1 2 3 4)')), ST_ZmFlag(ST_GeomFromText('POINT ZM(1 2 3 4)')), ST_Z(ST_GeomFromText('POINT ZM(1 2 3 4)')), ST_M(ST_GeomFromText('POINT ZM(1 2 3 4)'));"

    // --- 10.7  ST_NDims / ST_ZmFlag on non-point 3D geometries ---
    qt_sql "SELECT ST_NDims(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('LINESTRING Z(0 0 1, 1 1 2)'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'));"
    qt_sql "SELECT ST_ZmFlag(ST_GeomFromText('POLYGON Z((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0))'));"
    qt_sql "SELECT ST_NDims(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)))'));"

    // --- 10.8  ST_Z / ST_M on Circle (Doris-specific type) ---
    qt_sql "SELECT ST_Z(ST_Circle(0, 0, 1));"
    qt_sql "SELECT ST_M(ST_Circle(0, 0, 1));"
    qt_sql "SELECT ST_NDims(ST_Circle(0, 0, 1));"
    qt_sql "SELECT ST_ZmFlag(ST_Circle(0, 0, 1));"

    // cleanup
    sql "DROP TABLE IF EXISTS geo_3d_test;"
}
