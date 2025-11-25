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

// use C++ parser skeleton
%language "c++"
//use strong type, class encapsulated LALR(1) parser
%skeleton "lalr1.cc"
//store all values in std::variant (strong type)
%define api.value.type variant
//automatically generate constructor for tokens (convenient for C++)
%define api.token.constructor
//generated parser class is `wkt_::parser`
%define api.prefix {wkt_}
%defines

/* ------------------------------
 * 1. Headers and forward declarations placed in the .hpp file
 *    **Generate location**：wkt_yacc.y.hpp(beginning)
 *    **Useage**：Provide types and forward declarations for the parser and external callers
 * ------------------------------ */
%code requires {
#include <memory>
#include <variant>
#include <vector>
#include "geo/wkt_parse_ctx.h"   // we need WktParseContext to pass scaninfo to lexer
#include "common/logging.h"
#include "geo/wkt_parse_type.h"
#include "geo/geo_types.h"

struct WktParseContext;
}

/* ------------------------------
 * 2. The function declarations provided to the lexer for calls are placed in the .hpp file.
 *    **Generate location**：wkt_yacc.y.hpp(ending)
 *    **Useage**：The lexer interacts with the parser, returning tokens.
 * ------------------------------ */
%code provides {
wkt_::parser::symbol_type wkt_lex(WktParseContext* ctx, yyscan_t scanner);
}

/* ------------------------------
 * 3. The parser member function implementation is placed in the .cpp file.
 *    **Generate location**：wkt_yacc.y.cpp
 *    **Useage**：Parser internal use, for example error() implementation
 * ------------------------------ */
%code {
#include <string>
#include "common/exception.h"

namespace wkt_ {
    // For syntax errors(e.g., `POIN(1 2)`, `POINT(1, 2)`), parser::error() does nothing.
    // The parse_wkt() function will set ctx.parse_status to GEO_PARSE_WKT_SYNTAX_ERROR.
    // For semantic errors (e.g., invalid coordinates), ctx.parse_status is set
    // in the grammar rules before calling YYABORT.
    void parser::error(const std::string& msg) {}

} // namespace wkt_
}

%parse-param { WktParseContext* ctx }
%parse-param { yyscan_t scanner }
%lex-param { WktParseContext* ctx }
%lex-param { yyscan_t scanner }

%expect 0

%start shape

/* keyword for */
%token KW_POINT KW_LINESTRING KW_POLYGON
%token KW_MULTI_POINT KW_MULTI_LINESTRING KW_MULTI_POLYGON

%token <double> NUMERIC

%type <std::monostate> shape
%type <std::unique_ptr<doris::GeoShape>> point linestring polygon multi_polygon
%type <std::unique_ptr<doris::GeoCoordinate>> coordinate
%type <std::unique_ptr<doris::GeoCoordinateList>> coordinate_list
%type <std::unique_ptr<doris::GeoCoordinateListList>> coordinate_list_list
%type <std::unique_ptr<std::vector<doris::GeoCoordinateListList>>> multi_polygon_list

%%

shape:
    point 
    { ctx->shape = std::move($1); }
    | linestring
    { ctx->shape = std::move($1); }
    | polygon
    { ctx->shape = std::move($1); }
    | multi_polygon
    { ctx->shape = std::move($1); }
    ;

point:
     KW_POINT '(' coordinate ')'
     {
        std::unique_ptr<doris::GeoPoint> point = doris::GeoPoint::create_unique();
        const doris::GeoCoordinate& coord = *$3;
        ctx->parse_status = point->from_coord(coord);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        std::unique_ptr<doris::GeoShape> shape = std::move(point);
        yylhs.value.emplace<std::unique_ptr<doris::GeoShape>>(std::move(shape));
     }
     ;

linestring:
    KW_LINESTRING '(' coordinate_list ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateList> list = std::move($3);
        std::unique_ptr<doris::GeoLine> line = doris::GeoLine::create_unique();
        ctx->parse_status = line->from_coords(*list);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        std::unique_ptr<doris::GeoShape> shape = std::move(line);
        yylhs.value.emplace<std::unique_ptr<doris::GeoShape>>(std::move(shape));
    }
    ;

polygon:
    KW_POLYGON '(' coordinate_list_list ')'
    {
        // to avoid memory leak
        std::unique_ptr<doris::GeoCoordinateListList> list = std::move($3);
        std::unique_ptr<doris::GeoPolygon> polygon = doris::GeoPolygon::create_unique();
        ctx->parse_status = polygon->from_coords(*list);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        std::unique_ptr<doris::GeoShape> shape = std::move(polygon);
        yylhs.value.emplace<std::unique_ptr<doris::GeoShape>>(std::move(shape));
    }
    ;

multi_polygon:
    KW_MULTI_POLYGON '(' multi_polygon_list ')'
    {
        // to avoid memory leak
        std::unique_ptr<std::vector<doris::GeoCoordinateListList>> list = std::move($3);
        std::unique_ptr<doris::GeoMultiPolygon> multi_polygon = doris::GeoMultiPolygon::create_unique();
        ctx->parse_status = multi_polygon->from_coords(*list);
        if (ctx->parse_status != doris::GEO_PARSE_OK) {
            YYABORT;
        }
        std::unique_ptr<doris::GeoShape> shape = std::move(multi_polygon);
        yylhs.value.emplace<std::unique_ptr<doris::GeoShape>>(std::move(shape));
    }
    ;

multi_polygon_list:
    multi_polygon_list ',' '(' coordinate_list_list ')'
    {
        auto vec = std::move($1);
        auto list = std::move($4);
        vec->push_back(std::move(*list));
        yylhs.value.emplace<std::unique_ptr<std::vector<doris::GeoCoordinateListList>>>(std::move(vec));
    }
    | '(' coordinate_list_list ')'
    {
        auto list = std::move($2);
        auto vec = std::make_unique<std::vector<doris::GeoCoordinateListList>>();
        vec->push_back(std::move(*list));
        yylhs.value.emplace<std::unique_ptr<std::vector<doris::GeoCoordinateListList>>>(std::move(vec));
    }
    ;

coordinate_list_list:
    coordinate_list_list ',' '(' coordinate_list ')'
    {
        auto outer = std::move($1);
        auto inner = std::move($4);
        outer->add(std::move(inner));
        yylhs.value.emplace<std::unique_ptr<doris::GeoCoordinateListList>>(std::move(outer));
    }
    | '(' coordinate_list ')'
    {
        auto inner = std::move($2);
        auto outer = std::make_unique<doris::GeoCoordinateListList>();
        outer->add(std::move(inner));
        yylhs.value.emplace<std::unique_ptr<doris::GeoCoordinateListList>>(std::move(outer));
    }
    ;

coordinate_list:
    coordinate_list ',' coordinate
    { 
        auto list = std::move($1);
        auto coord = std::move($3);
        list->add(*coord);
        yylhs.value.emplace<std::unique_ptr<doris::GeoCoordinateList>>(std::move(list));
    }
    | coordinate
    {
        auto coord = std::move($1);
        auto list = std::make_unique<doris::GeoCoordinateList>();
        list->add(*coord);
        yylhs.value.emplace<std::unique_ptr<doris::GeoCoordinateList>>(std::move(list));
    }
    ;

coordinate:
    NUMERIC NUMERIC
    {
        auto coord = std::make_unique<doris::GeoCoordinate>();
        coord->x = $1;
        coord->y = $2;
        yylhs.value.emplace<std::unique_ptr<doris::GeoCoordinate>>(std::move(coord));
    }
    ;

