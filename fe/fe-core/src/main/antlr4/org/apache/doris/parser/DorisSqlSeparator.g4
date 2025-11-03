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

grammar DorisSqlSeparator;

statements : statement (SEPARATOR statement)* EOF ;

statement
    : (comment | string | quoteIdentifier | ws | someText)+
    | // empty statement
    ;

quoteIdentifier
    : '`' (~('`') | '``')* '`'
    ;

string
    : SINGLE_QUOTE_STRING
    | DOUBLE_QUOTE_STRING
    ;

comment
    : TRADITIONAL_COMMENT
    | END_OF_LINE_COMMENT
    ;

ws: WHITESPACE+;
someText: NON_SEPARATOR+;

WHITESPACE: ' ' | '\t' | '\f' | LINE_TERMINATOR;
SINGLE_QUOTE_STRING: '\'' ( ~('\''|'\\') | ('\\' .) )* '\'';
DOUBLE_QUOTE_STRING: '"' ( ~('"'|'\\') | ('\\' .) )* '"';
TRADITIONAL_COMMENT: '/*' .*? '*/' ;
END_OF_LINE_COMMENT: '--' (~[\r\n])* LINE_TERMINATOR? ;
NON_SEPARATOR: (~';');
SEPARATOR: ';';

fragment LINE_TERMINATOR: '\r' | '\n' | '\r\n';
