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

suite("test_st_num_geometries_num_points_and_geometries", "arrow_flight_sql") {
    sql "set batch_size = 4096;"

    // ----------------------------------------------------------------------
    // ST_NumPoints: simple geometry inputs
    // ----------------------------------------------------------------------

    qt_st_numpoints_point_constructor """
        select ST_NumPoints(ST_Point(1, 2));
    """

    qt_st_numpoints_point_wkt """
        select ST_NumPoints(ST_GeomFromText('POINT(1 2)'));
    """

    qt_st_numpoints_linestring_two_points """
        select ST_NumPoints(ST_GeomFromText('LINESTRING(0 0, 1 1)'));
    """

    qt_st_numpoints_linestring_three_points """
        select ST_NumPoints(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'));
    """

    qt_st_numpoints_linestring_four_points """
        select ST_NumPoints(ST_GeomFromText('LINESTRING(0 0, 1 0, 1 1, 0 1)'));
    """

    qt_st_numpoints_triangle_polygon """
        select ST_NumPoints(ST_GeomFromText('POLYGON((0 0, 1 0, 0 1, 0 0))'));
    """

    qt_st_numpoints_square_polygon """
        select ST_NumPoints(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
    """

    qt_st_numpoints_polygon_with_one_hole """
        select ST_NumPoints(ST_GeomFromText(
            'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'
        ));
    """

    qt_st_numpoints_polygon_with_two_holes """
        select ST_NumPoints(ST_GeomFromText(
            'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (7 7, 7 8, 8 8, 8 7, 7 7))'
        ));
    """

    qt_st_numpoints_multipolygon_one_polygon """
        select ST_NumPoints(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))'
        ));
    """

    qt_st_numpoints_multipolygon_two_polygons """
        select ST_NumPoints(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        ));
    """

    qt_st_numpoints_multipolygon_three_polygons """
        select ST_NumPoints(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)), ((5 5, 6 5, 5 6, 5 5)))'
        ));
    """

    qt_st_numpoints_multipolygon_with_hole """
        select ST_NumPoints(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((20 20, 20 21, 21 21, 21 20, 20 20)))'
        ));
    """

    qt_st_numpoints_circle """
        select ST_NumPoints(ST_Circle(0, 0, 1000));
    """

    // ----------------------------------------------------------------------
    // ST_NumGeometries: simple geometry and multi geometry inputs
    // ----------------------------------------------------------------------

    qt_st_numgeometries_point_constructor """
        select ST_NumGeometries(ST_Point(1, 2));
    """

    qt_st_numgeometries_point_wkt """
        select ST_NumGeometries(ST_GeomFromText('POINT(1 2)'));
    """

    qt_st_numgeometries_linestring """
        select ST_NumGeometries(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'));
    """

    qt_st_numgeometries_polygon """
        select ST_NumGeometries(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
    """

    qt_st_numgeometries_polygon_with_hole """
        select ST_NumGeometries(ST_GeomFromText(
            'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'
        ));
    """

    qt_st_numgeometries_multipolygon_one_polygon """
        select ST_NumGeometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))'
        ));
    """

    qt_st_numgeometries_multipolygon_two_polygons """
        select ST_NumGeometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        ));
    """

    qt_st_numgeometries_multipolygon_three_polygons """
        select ST_NumGeometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)), ((5 5, 6 5, 5 6, 5 5)))'
        ));
    """

    qt_st_numgeometries_multipolygon_with_hole """
        select ST_NumGeometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((20 20, 20 21, 21 21, 21 20, 20 20)))'
        ));
    """

    qt_st_numgeometries_circle """
        select ST_NumGeometries(ST_Circle(0, 0, 1000));
    """

    // ----------------------------------------------------------------------
    // ST_Geometries: simple geometry should return array with one element
    // ----------------------------------------------------------------------

    qt_st_geometries_point_size """
        select ARRAY_SIZE(ST_Geometries(ST_Point(1, 2)));
    """

    qt_st_geometries_point_element_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_Point(1, 2)), 1));
    """

    qt_st_geometries_point_element_text """
        select ST_AsText(ELEMENT_AT(ST_Geometries(ST_Point(1, 2)), 1));
    """

    qt_st_geometries_linestring_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')));
    """

    qt_st_geometries_linestring_element_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')), 1));
    """

    qt_st_geometries_linestring_element_text """
        select ST_AsText(ELEMENT_AT(ST_Geometries(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')), 1));
    """

    qt_st_geometries_polygon_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')));
    """

    qt_st_geometries_polygon_element_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 1));
    """

    qt_st_geometries_polygon_element_text """
        select ST_AsText(ELEMENT_AT(ST_Geometries(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 1));
    """

    qt_st_geometries_polygon_with_hole_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText(
            'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'
        )));
    """

    qt_st_geometries_polygon_with_hole_element_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'
        )), 1));
    """

    // ----------------------------------------------------------------------
    // ST_Geometries: multipolygon should be split into polygon elements
    // ----------------------------------------------------------------------

    qt_st_geometries_multipolygon_one_polygon_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))'
        )));
    """

    qt_st_geometries_multipolygon_one_polygon_element_1_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))'
        )), 1));
    """

    qt_st_geometries_multipolygon_two_polygons_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )));
    """

    qt_st_geometries_multipolygon_two_polygons_element_1_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )), 1));
    """

    qt_st_geometries_multipolygon_two_polygons_element_2_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )), 2));
    """

    qt_st_geometries_multipolygon_two_polygons_element_1_text """
        select ST_AsText(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )), 1));
    """

    qt_st_geometries_multipolygon_two_polygons_element_2_text """
        select ST_AsText(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )), 2));
    """

    qt_st_geometries_multipolygon_two_polygons_element_3_out_of_bound """
        select ST_AsText(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )), 3));
    """

    qt_st_geometries_multipolygon_three_polygons_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)), ((5 5, 6 5, 5 6, 5 5)))'
        )));
    """

    qt_st_geometries_multipolygon_three_polygons_element_3_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)), ((5 5, 6 5, 5 6, 5 5)))'
        )), 3));
    """

    qt_st_geometries_multipolygon_with_hole_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((20 20, 20 21, 21 21, 21 20, 20 20)))'
        )));
    """

    qt_st_geometries_multipolygon_with_hole_element_1_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((20 20, 20 21, 21 21, 21 20, 20 20)))'
        )), 1));
    """

    qt_st_geometries_multipolygon_with_hole_element_2_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((20 20, 20 21, 21 21, 21 20, 20 20)))'
        )), 2));
    """

    // ----------------------------------------------------------------------
    // ST_Geometries: circle behavior
    // Circle is a Doris-specific shape. It should be treated as one geometry.
    // ----------------------------------------------------------------------

    qt_st_geometries_circle_size """
        select ARRAY_SIZE(ST_Geometries(ST_Circle(0, 0, 1000)));
    """

    qt_st_geometries_circle_element_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_Circle(0, 0, 1000)), 1));
    """

    qt_st_geometries_circle_element_2_out_of_bound """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_Circle(0, 0, 1000)), 2));
    """

    // ----------------------------------------------------------------------
    // NULL and invalid inputs
    // ----------------------------------------------------------------------

    qt_st_numpoints_null_literal """
        select ST_NumPoints(null);
    """

    qt_st_numgeometries_null_literal """
        select ST_NumGeometries(null);
    """

    qt_st_geometries_null_literal_size """
        select ARRAY_SIZE(ST_Geometries(null));
    """

    qt_st_numpoints_geomfromtext_null """
        select ST_NumPoints(ST_GeomFromText(null));
    """

    qt_st_numgeometries_geomfromtext_null """
        select ST_NumGeometries(ST_GeomFromText(null));
    """

    qt_st_geometries_geomfromtext_null_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText(null)));
    """

    qt_st_numpoints_invalid_encoded_string """
        select ST_NumPoints('abc');
    """

    qt_st_numgeometries_invalid_encoded_string """
        select ST_NumGeometries('abc');
    """

    qt_st_geometries_invalid_encoded_string_size """
        select ARRAY_SIZE(ST_Geometries('abc'));
    """

    qt_st_numpoints_invalid_wkt """
        select ST_NumPoints(ST_GeomFromText('INVALID(0 0)'));
    """

    qt_st_numgeometries_invalid_wkt """
        select ST_NumGeometries(ST_GeomFromText('INVALID(0 0)'));
    """

    qt_st_geometries_invalid_wkt_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText('INVALID(0 0)')));
    """

    qt_st_numpoints_invalid_polygon_not_closed """
        select ST_NumPoints(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1))'));
    """

    qt_st_numgeometries_invalid_polygon_not_closed """
        select ST_NumGeometries(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1))'));
    """

    qt_st_geometries_invalid_polygon_not_closed_size """
        select ARRAY_SIZE(ST_Geometries(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1))')));
    """

    // ----------------------------------------------------------------------
    // Lowercase function names and ST_GeometryFromText alias coverage
    // ----------------------------------------------------------------------

    qt_st_numpoints_lowercase_name """
        select st_numpoints(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'));
    """

    qt_st_numgeometries_lowercase_name """
        select st_numgeometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        ));
    """

    qt_st_geometries_lowercase_name_size """
        select ARRAY_SIZE(st_geometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )));
    """

    qt_st_numpoints_geometryfromtext_alias """
        select ST_NumPoints(ST_GeometryFromText('LINESTRING(0 0, 1 1, 2 2, 3 3)'));
    """

    qt_st_numgeometries_geometryfromtext_alias """
        select ST_NumGeometries(ST_GeometryFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
    """

    qt_st_geometries_geometryfromtext_alias_element_2_type """
        select ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeometryFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )), 2));
    """

    // ----------------------------------------------------------------------
    // Table-driven tests: column input, ordering, NULL row, mixed geometry types
    // ----------------------------------------------------------------------

    sql "drop table if exists test_st_geometry_component_functions_cases"
    sql """
    CREATE TABLE test_st_geometry_component_functions_cases (
        `id` int NOT NULL,
        `wkt` varchar(2048) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 4
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    insert into test_st_geometry_component_functions_cases values
        (1, 'POINT(0 0)'),
        (2, 'LINESTRING(0 0, 0 1)'),
        (3, 'LINESTRING(0 0, 1 0, 1 1, 0 1)'),
        (4, 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
        (5, 'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 3, 3 3, 3 1, 1 1))'),
        (6, 'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))'),
        (7, 'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'),
        (8, 'MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((20 20, 20 21, 21 21, 21 20, 20 20)))'),
        (9, null);
    """

    qt_st_num_points_table_ordered """
        select id, ST_NumPoints(ST_GeomFromText(wkt))
        from test_st_geometry_component_functions_cases
        order by id;
    """

    qt_st_num_geometries_table_ordered """
        select id, ST_NumGeometries(ST_GeomFromText(wkt))
        from test_st_geometry_component_functions_cases
        order by id;
    """

    qt_st_geometries_table_size_ordered """
        select id, ARRAY_SIZE(ST_Geometries(ST_GeomFromText(wkt)))
        from test_st_geometry_component_functions_cases
        order by id;
    """

    qt_st_geometries_table_first_element_type_ordered """
        select id, ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(wkt)), 1))
        from test_st_geometry_component_functions_cases
        order by id;
    """

    qt_st_geometries_table_second_element_type_ordered """
        select id, ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(wkt)), 2))
        from test_st_geometry_component_functions_cases
        order by id;
    """

    qt_st_all_three_functions_table_summary_ordered """
        select
            id,
            ST_GeometryType(ST_GeomFromText(wkt)),
            ST_NumPoints(ST_GeomFromText(wkt)),
            ST_NumGeometries(ST_GeomFromText(wkt)),
            ARRAY_SIZE(ST_Geometries(ST_GeomFromText(wkt))),
            ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(wkt)), 1)),
            ST_GeometryType(ELEMENT_AT(ST_Geometries(ST_GeomFromText(wkt)), 2))
        from test_st_geometry_component_functions_cases
        order by id;
    """

    // ----------------------------------------------------------------------
    // Return values used in arithmetic and predicates
    // ----------------------------------------------------------------------

    qt_st_numpoints_arithmetic """
        select ST_NumPoints(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')) + 10;
    """

    qt_st_numgeometries_arithmetic """
        select ST_NumGeometries(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        )) + 10;
    """

    qt_st_numpoints_predicate """
        select id
        from test_st_geometry_component_functions_cases
        where ST_NumPoints(ST_GeomFromText(wkt)) >= 5
        order by id;
    """

    qt_st_numgeometries_predicate """
        select id
        from test_st_geometry_component_functions_cases
        where ST_NumGeometries(ST_GeomFromText(wkt)) >= 2
        order by id;
    """

    qt_st_geometries_predicate """
        select id
        from test_st_geometry_component_functions_cases
        where ARRAY_SIZE(ST_Geometries(ST_GeomFromText(wkt))) >= 2
        order by id;
    """

    // ----------------------------------------------------------------------
    // ST_NPoints: alias for ST_NumPoints
    // ----------------------------------------------------------------------

    qt_st_npoints_point """
        select ST_NPoints(ST_Point(1, 2));
    """

    qt_st_npoints_linestring """
        select ST_NPoints(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'));
    """

    qt_st_npoints_polygon """
        select ST_NPoints(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));
    """

    qt_st_npoints_multipolygon """
        select ST_NPoints(ST_GeomFromText(
            'MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))'
        ));
    """

    qt_st_npoints_circle """
        select ST_NPoints(ST_Circle(0, 0, 1000));
    """

    qt_st_npoints_null """
        select ST_NPoints(null);
    """

    qt_st_npoints_invalid """
        select ST_NPoints('abc');
    """

    qt_st_npoints_table_ordered """
        select id, ST_NPoints(ST_GeomFromText(wkt))
        from test_st_geometry_component_functions_cases
        order by id;
    """
}
