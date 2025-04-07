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
#include "geo/geo_types.h"
#include <memory>

struct WktParseContext;
void wkt_error(WktParseContext* ctx, const char* msg) {
}
/* forward declare this class for wkt_parse declaration in yacc.y.cpp */
%}

%union {
    double double_value;
    doris::GeoCoordinate coordinate_value;
    doris::GeoCoordinateList* coordinate_list_ptr;
    doris::GeoCoordinateListUPtrList* coordinate_list_ptr_list_ptr;
    doris::GeoShape* shape_value;
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
%token KW_POINT KW_LINESTRING KW_POLYGON
%token KW_MULTI_POINT KW_MULTI_LINESTRING KW_MULTI_POLYGON

%token <double_value> NUMERIC

%type <None> shape
%type <shape_value> point linestring polygon
%type <coordinate_value> coordinate
%type <coordinate_list_ptr> coordinate_list
%type <coordinate_list_ptr_list_ptr> coordinate_list_list

%%

shape:
    point
    { ctx->shape = std::unique_ptr<doris::GeoShape>($1); }
    | linestring
    { ctx->shape = std::unique_ptr<doris::GeoShape>($1); }
    | polygon
    { ctx->shape = std::unique_ptr<doris::GeoShape>($1); }
    ;

point: // doris::GeoShape*
     KW_POINT '(' coordinate ')'
     {
        auto point = doris::GeoPoint::create_unique().release();
        ctx->parse_status = point->from_coord($3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = point;
     }
     ;

linestring: // doris::GeoShape*
    KW_LINESTRING '(' coordinate_list ')'
    {
        auto line = doris::GeoLine::create_unique().release();
        ctx->parse_status = line->from_coords(*$3);
        delete $3;
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = line;
    }
    ;

polygon: // doris::GeoShape*
    KW_POLYGON '(' coordinate_list_list ')'
    {
        auto polygon = doris::GeoPolygon::create_unique().release();
        ctx->parse_status = polygon->from_coords(*$3);
        delete $3;
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = polygon;
    }
    ;

coordinate_list_list: // doris::GeoCoordinateListUPtrList*
    coordinate_list_list ',' '(' coordinate_list ')'
    {
        auto coord_list_sptr = std::unique_ptr<doris::GeoCoordinateList>($4);
        $1->push_back(std::move(coord_list_sptr));
        $$ = $1;
    }
    | '(' coordinate_list ')'
    {
        auto coord_list_sptr = std::unique_ptr<doris::GeoCoordinateList>($2);
        $$ = new doris::GeoCoordinateListUPtrList();
        $$->push_back(std::move(coord_list_sptr));
    }
    ;

coordinate_list: // doris::GeoCoordinateList*
    coordinate_list ',' coordinate
    { 
        $1->push_back($3);
        $$ = $1;
    }
    | coordinate
    {
        $$ = new doris::GeoCoordinateList();
        $$->push_back($1);
    }
    ;

coordinate: // GeoCoordinate
    NUMERIC NUMERIC
    {
        $$.x = $1;
        $$.y = $2;
    }
    ;

