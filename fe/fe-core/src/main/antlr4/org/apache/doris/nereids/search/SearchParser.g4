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

parser grammar SearchParser;

options { tokenVocab=SearchLexer; }

search     : clause EOF ;
clause     : orClause ;
orClause   : andClause (OR andClause)* ;
andClause  : notClause (AND notClause)* ;
notClause  : NOT atomClause | atomClause ;
atomClause : LPAREN clause RPAREN | fieldQuery ;

// Support for variant subcolumn paths (e.g., field.subcolumn, field.sub1.sub2)
fieldQuery : fieldPath COLON searchValue ;
fieldPath  : fieldSegment (DOT fieldSegment)* ;
fieldSegment : TERM | QUOTED ;

searchValue
    : TERM
    | PREFIX
    | WILDCARD
    | REGEXP
    | QUOTED
    | rangeValue
    | listValue
    | anyAllValue
    | exactValue
    ;

rangeValue
    : LBRACKET rangeEndpoint RANGE_TO rangeEndpoint RBRACKET
    | LBRACE rangeEndpoint RANGE_TO rangeEndpoint RBRACE
    ;

rangeEndpoint : RANGE_NUMBER | RANGE_STAR ;

listValue   : IN_LPAREN LIST_TERM* LIST_RPAREN ;
anyAllValue : (ANY_LPAREN | ALL_LPAREN) STRING_CONTENT? STRING_RPAREN ;
exactValue  : EXACT_LPAREN STRING_CONTENT? STRING_RPAREN ;