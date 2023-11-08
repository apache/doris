/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

%{
#include "common/logging.h"
#include "geo/wkt_parse_type.h"
#include "geo/util/GeoShape.h"
#include "geo/util/GeoPoint.h"
#include "geo/util/GeoLineString.h"
#include "geo/util/GeoPolygon.h"
#include "geo/util/GeoMultiPoint.h"
#include "geo/util/GeoMultiLineString.h"
#include "geo/util/GeoMultiPolygon.h"
#include "geo/util/GeoCollection.h"
#include "geo/util/GeoCircle.h"

struct WktParseContext;
void wkt_error(WktParseContext* ctx, const char* msg) {
}
/* forward declare this class for wkt_parse declaration in yacc.y.cpp */
%}

%union {
    double double_value;
    doris::GeoCoordinate coordinate_value;
    doris::GeoCoordinates* coordinates_value;
    doris::GeoCoordinateLists* coordinate_lists_value;
    doris::GeoShape* shape_value;
    doris::GeoMultiPoint* point_collection_value;
    doris::GeoMultiLineString* linestring_collection_value;
    doris::GeoMultiPolygon* polygon_collection_value;
    doris::GeoCollection* collection_value;
}

%code {
/* we need yyscan_t in WktParseContext, so we include lex.h here,
 * and we should include this header after union define, because it
 * need YYSTYPE
 */
#include "geo/wkt_lex.l.h"
/* we need WktParseContext to pass scaninfo to lexer */
#include "geo/wkt_parse_ctx.h"

#define WKT_LEX_PARAM ctx->scaninfo
}

%define api.pure full
%parse-param { WktParseContext* ctx }
%lex-param { WKT_LEX_PARAM }

/* for multi-thread */
%define api.prefix {wkt_}
%defines

%expect 0

%start shape

/* keyword for */
%token KW_POINT KW_POINT_EMPTY KW_LINESTRING KW_LINESTRING_EMPTY KW_POLYGON KW_POLYGON_EMPTY KW_EMPTY
%token KW_MULTI_POINT KW_MULTI_LINESTRING KW_MULTI_POLYGON KW_GEOMETRY_COLLECTION
%token KW_MULTI_POINT_EMPTY KW_MULTI_LINESTRING_EMPTY KW_MULTI_POLYGON_EMPTY KW_GEOMETRY_COLLECTION_EMPTY

%token <double_value> NUMERIC

%type <None> shape
%type <shape_value> point point_empty linestring linestring_empty polygon polygon_empty multipoint multipoint_empty multilinestring multilinestring_empty multipolygon multipolygon_empty geometrycollection geometrycollection_empty
%type <collection_value> collections
%type <point_collection_value> point_collections
%type <linestring_collection_value> linestring_collections
%type <polygon_collection_value> polygon_collections
%type <coordinate_value> coordinate
%type <coordinates_value> coordinates
%type <coordinate_lists_value> coordinate_lists

%destructor { delete $$; } coordinates
%destructor { delete $$; } coordinate_lists
%destructor { delete $$; } point
%destructor { delete $$; } point_empty
%destructor { delete $$; } linestring
%destructor { delete $$; } linestring_empty
%destructor { delete $$; } polygon
%destructor { delete $$; } polygon_empty
%destructor { delete $$; } multipoint
%destructor { delete $$; } multipoint_empty
%destructor { delete $$; } multilinestring
%destructor { delete $$; } multilinestring_empty
%destructor { delete $$; } multipolygon
%destructor { delete $$; } multipolygon_empty
%destructor { delete $$; } geometrycollection
%destructor { delete $$; } geometrycollection_empty
%destructor { delete $$; } collections
%destructor { delete $$; } point_collections
%destructor { delete $$; } linestring_collections
%%

shape:
    point
    { ctx->shape = $1; }
    | point_empty
    { ctx->shape = $1; }
    | linestring
    { ctx->shape = $1; }
    | linestring_empty
    { ctx->shape = $1; }
    | polygon
    { ctx->shape = $1; }
    | polygon_empty
    { ctx->shape = $1; }
    | multipoint
    { ctx->shape = $1; }
    | multipoint_empty
    { ctx->shape = $1; }
    | multilinestring
    { ctx->shape = $1; }
    | multilinestring_empty
    { ctx->shape = $1; }
    | multipolygon
    { ctx->shape = $1; }
    | multipolygon_empty
    { ctx->shape = $1; }
    | geometrycollection
    { ctx->shape = $1; }
    | geometrycollection_empty
    { ctx->shape = $1; }
    ;

point:
     KW_POINT '(' coordinate ')'
     {
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        ctx->parse_status = point->from_coord($3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = point.release();
     }
     ;

point_empty:
     KW_POINT_EMPTY
     {
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        point->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = point.release();
     }
     ;

linestring:
    KW_LINESTRING '(' coordinates ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinates> list($3);
        std::unique_ptr<doris::GeoLineString> line = doris::GeoLineString::create_unique();
        ctx->parse_status = line->from_coords(*$3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = line.release();
    }
    ;

linestring_empty:
     KW_LINESTRING_EMPTY
     {
        std::unique_ptr<doris::GeoLineString> line = doris::GeoLineString::create_unique();
        line->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = line.release();
     }
     ;

polygon:
    KW_POLYGON '(' coordinate_lists ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateLists> list($3);
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        ctx->parse_status = polygon->from_coords(*$3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = polygon.release();
    }
    ;

polygon_empty:
     KW_POLYGON_EMPTY
     {
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        polygon->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = polygon.release();
     }
     ;

multipoint:
    KW_MULTI_POINT '(' point_collections ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoMultiPoint> multipoint = doris::GeoMultiPoint::create_unique();
        multipoint.reset($3);
        $$ = multipoint.release();
    }
    ;

multipoint_empty:
     KW_MULTI_POINT_EMPTY
     {
        std::unique_ptr<doris::GeoMultiPoint> multipoint = doris::GeoMultiPoint::create_unique();
        multipoint->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = multipoint.release();
     }
     ;

multilinestring:
    KW_MULTI_LINESTRING '(' linestring_collections ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoMultiLineString> multilinestring = doris::GeoMultiLineString::create_unique();
        multilinestring.reset($3);
        $$ = multilinestring.release();
    }
    ;

multilinestring_empty:
     KW_MULTI_LINESTRING_EMPTY
     {
        std::unique_ptr<doris::GeoMultiLineString> multilinestring = doris::GeoMultiLineString::create_unique();
        multilinestring->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = multilinestring.release();
     }
     ;

multipolygon:
    KW_MULTI_POLYGON '(' polygon_collections ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoMultiPolygon> multipolygon = doris::GeoMultiPolygon::create_unique();
        multipolygon.reset($3);
        $$ = multipolygon.release();
    }
    ;

multipolygon_empty:
     KW_MULTI_POLYGON_EMPTY
     {
        std::unique_ptr<doris::GeoMultiPolygon> multipolygon = doris::GeoMultiPolygon::create_unique();
        multipolygon->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = multipolygon.release();
     }
     ;

geometrycollection:
    KW_GEOMETRY_COLLECTION '(' collections ')'
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        collection.reset($3);
        $$ = collection.release();
    }
    ;

geometrycollection_empty:
     KW_GEOMETRY_COLLECTION_EMPTY
     {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        collection->set_empty();
        ctx->parse_status = doris::GEO_PARSE_OK;
        $$ = collection.release();
     }
     ;

point_collections:
    point_collections ',' coordinate
    {
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        ctx->parse_status = point->from_coord($3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        ctx->parse_status = $1->add_one_geometry(point.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = $1;
    }
    | point_collections ',' KW_EMPTY
    {
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        point->set_empty();
        $1->add_one_geometry(point.release());
        $$ = $1;
    }
    | coordinate
    {
        std::unique_ptr<doris::GeoMultiPoint> multi_point = doris::GeoMultiPoint::create_unique();
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        ctx->parse_status = point->from_coord($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        ctx->parse_status = multi_point->add_one_geometry(point.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = multi_point.release();
    }
    | KW_EMPTY
    {
        std::unique_ptr<doris::GeoMultiPoint> multi_point = doris::GeoMultiPoint::create_unique();
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        point->set_empty();
        ctx->parse_status = multi_point->add_one_geometry(point.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = multi_point.release();
    }
    ;

linestring_collections:
    linestring_collections ',' '(' coordinates ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinates> list($4);
        std::unique_ptr<doris::GeoLineString> linestring = doris::GeoLineString::create_unique();
        ctx->parse_status = linestring->from_coords(*$4);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        ctx->parse_status = $1->add_one_geometry(linestring.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = $1;
    }
    | linestring_collections ',' KW_EMPTY
    {
        std::unique_ptr<doris::GeoLineString> linestring = doris::GeoLineString::create_unique();
        linestring->set_empty();
        ctx->parse_status = $1->add_one_geometry(linestring.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = $1;
    }
    | '(' coordinates ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinates> list($2);
        std::unique_ptr<doris::GeoMultiLineString> multi_linestring = doris::GeoMultiLineString::create_unique();
        std::unique_ptr<doris::GeoLineString> linestring = doris::GeoLineString::create_unique();
        ctx->parse_status = linestring->from_coords(*$2);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        ctx->parse_status = multi_linestring->add_one_geometry(linestring.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = multi_linestring.release();
    }
    | KW_EMPTY
    {
        std::unique_ptr<doris::GeoMultiLineString> multi_linestring = doris::GeoMultiLineString::create_unique();
        std::unique_ptr<doris::GeoLineString> linestring = doris::GeoLineString::create_unique();
        linestring->set_empty();
        ctx->parse_status = multi_linestring->add_one_geometry(linestring.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = multi_linestring.release();
    }
    ;

polygon_collections:
    polygon_collections ',' '(' coordinate_lists ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateLists> list($4);
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        ctx->parse_status = polygon->from_coords(*$4);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        ctx->parse_status = $1->add_one_geometry(polygon.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = $1;
    }
    | polygon_collections ',' KW_EMPTY
    {
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        polygon->set_empty();
        $1->add_one_geometry(polygon.release());
        $$ = $1;
    }
    | '(' coordinate_lists ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateLists> list($2);
        std::unique_ptr<doris::GeoMultiPolygon> multi_polygon = doris::GeoMultiPolygon::create_unique();
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        ctx->parse_status = polygon->from_coords(*$2);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        ctx->parse_status = multi_polygon->add_one_geometry(polygon.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = multi_polygon.release();
    }
    | KW_EMPTY
    {
        std::unique_ptr<doris::GeoMultiPolygon> multi_polygon = doris::GeoMultiPolygon::create_unique();
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        polygon->set_empty();
        ctx->parse_status = multi_polygon->add_one_geometry(polygon.release());
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = multi_polygon.release();
    }
    ;

collections:
    collections ',' point
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' point_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' linestring
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' linestring_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' polygon
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' polygon_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' multipoint
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' multipoint_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' multilinestring
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' multilinestring_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' multipolygon
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' multipolygon_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' geometrycollection
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | collections ',' geometrycollection_empty
    {
        $1->add_one_geometry($3);
        $$ = $1;
    }
    | point
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | point_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | linestring
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | linestring_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | polygon
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | polygon_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | multipoint
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | multipoint_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | multilinestring
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | multilinestring_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | multipolygon
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | multipolygon_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | geometrycollection
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    | geometrycollection_empty
    {
        std::unique_ptr<doris::GeoCollection> collection = doris::GeoCollection::create_unique();
        ctx->parse_status = collection->add_one_geometry($1);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = collection.release();
    }
    ;

coordinate_lists:
    coordinate_lists ',' '(' coordinates ')'
    {
        $1->add($4);
        $$ = $1;
    }
    | '(' coordinates ')'
    {
        $$ = new doris::GeoCoordinateLists();
        $$->add($2);
    }
    ;

coordinates:
    coordinates ',' coordinate
    {
        $1->add($3);
        $$ = $1;
    }
    | coordinate
    {
        $$ = new doris::GeoCoordinates();
        $$->add($1);
    }
    ;

coordinate:
    NUMERIC NUMERIC
    {
        $$.x = $1;
        $$.y = $2;
    }
    | '(' NUMERIC NUMERIC ')'
    {
        $$.x = $2;
        $$.y = $3;
    }
    ;

