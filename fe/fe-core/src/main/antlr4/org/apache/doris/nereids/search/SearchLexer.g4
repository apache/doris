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

lexer grammar SearchLexer;

// ============== Fragments ==============

fragment NUM_CHAR      : [0-9]+ ;
fragment ESCAPED_CHAR  : '\\' . ;
fragment NON_ESCAPED   : ~[\\)] ;

fragment TERM_START_CHAR
    : ~[ \t\n\r\u3000+\-!():^[\]"{}~*?\\/]
    | ESCAPED_CHAR
    ;

fragment TERM_CHAR
    : TERM_START_CHAR
    | '-'
    | '+'
    ;

fragment QUOTED_CHAR
    : ~["\\]
    | ESCAPED_CHAR
    ;

// ============== Default lexer rules ==============

AND : 'AND' | 'and' ;
OR  : 'OR' | 'or' ;
NOT : 'NOT' | 'not' | '!' ;

LPAREN   : '(' ;
RPAREN   : ')' ;
COLON    : ':' ;
DOT      : '.' ;  // Support for variant subcolumn access (e.g., field.subcolumn)

QUOTED   : '"' QUOTED_CHAR* '"' ;
TERM     : TERM_START_CHAR TERM_CHAR* ;
PREFIX   : '*' | TERM_START_CHAR TERM_CHAR* '*' ;
WILDCARD : (TERM_START_CHAR | '*' | '?') (TERM_CHAR | '*' | '?')* ;
REGEXP   : '/' (~[/] | '\\/')* '/' ;

LBRACKET : '[' -> pushMode(RANGE_MODE) ;
LBRACE   : '{' -> pushMode(RANGE_MODE) ;

IN_LPAREN    : [Ii][Nn] '(' -> pushMode(LIST_MODE) ;
ANY_LPAREN   : [Aa][Nn][Yy] '(' -> pushMode(STRING_MODE) ;
ALL_LPAREN   : [Aa][Ll][Ll] '(' -> pushMode(STRING_MODE) ;
EXACT_LPAREN : [Ee][Xx][Aa][Cc][Tt] '(' -> pushMode(STRING_MODE) ;

WS : [ \t\r\n\u3000]+ -> skip ;

// ============== Range lexer rules ==============

mode RANGE_MODE;

RANGE_TO     : 'TO' | 'to' ;
RANGE_NUMBER : '-'? [0-9]+ ('.' [0-9]+)? ;
RANGE_STAR   : '*' ;

RBRACKET : ']' -> popMode ;
RBRACE   : '}' -> popMode ;

RANGE_WS : [ \t\r\n\u3000]+ -> skip ;

// ============== List lexer rules ==============

mode LIST_MODE;

LIST_TERM : TERM_START_CHAR TERM_CHAR* ;
LIST_RPAREN : ')' -> popMode ;

LIST_WS : [ \t\r\n\u3000]+ -> skip ;

// ============== String lexer rules ==============

mode STRING_MODE;

STRING_CONTENT : (ESCAPED_CHAR | NON_ESCAPED)+ ;
STRING_RPAREN : ')' -> popMode ;