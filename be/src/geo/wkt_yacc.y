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

struct WktParseContext;
void wkt_error(WktParseContext* ctx, const char* msg) {
}
/* forward declare this class for wkt_parse declaration in yacc.y.cpp */
%}

%union {
    double double_value;
    doris::GeoCoordinate coordinate_value;
    doris::GeoCoordinateList* coordinate_list_value;
    doris::GeoCoordinateListList* coordinate_list_list_value;
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
%type <coordinate_list_value> coordinate_list
%type <coordinate_list_list_value> coordinate_list_list

%destructor { delete $$; } coordinate_list
%destructor { delete $$; } coordinate_list_list
%destructor { delete $$; } point
%destructor { delete $$; } linestring
%destructor { delete $$; } polygon

%%

shape:
    point 
    { ctx->shape = $1; }
    | linestring
    { ctx->shape = $1; }
    | polygon
    { ctx->shape = $1; }
    ;

point:
     KW_POINT '(' coordinate ')'
     {
        std::unique_ptr<doris::GeoPoint> point(new doris::GeoPoint());
        ctx->parse_status = point->from_coord($3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = point.release();
     }
     ;

linestring:
    KW_LINESTRING '(' coordinate_list ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateList> list($3);
        std::unique_ptr<doris::GeoLine> line(new doris::GeoLine());
        ctx->parse_status = line->from_coords(*$3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = line.release();
    }
    ;

polygon:
    KW_POLYGON '(' coordinate_list_list ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateListList> list($3);
        std::unique_ptr<doris::GeoPolygon> polygon(new doris::GeoPolygon());
        ctx->parse_status = polygon->from_coords(*$3);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        $$ = polygon.release();
    }
    ;

coordinate_list_list:
    coordinate_list_list ',' '(' coordinate_list ')'
    {
        $1->add($4);
        $$ = $1;
    }
    | '(' coordinate_list ')'
    {
        $$ = new doris::GeoCoordinateListList();
        $$->add($2);
    }
    ;

coordinate_list:
    coordinate_list ',' coordinate
    { 
        $1->add($3);
        $$ = $1;
    }
    | coordinate
    {
        $$ = new doris::GeoCoordinateList();
        $$->add($1);
    }
    ;

coordinate:
    NUMERIC NUMERIC
    {
        $$.x = $1;
        $$.y = $2;
    }
    ;

