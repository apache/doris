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

QUOTED   : '"' QUOTED_CHAR* '"' ;
TERM     : TERM_START_CHAR TERM_CHAR* ;
PREFIX   : '*' | TERM_START_CHAR TERM_CHAR* '*' ;
WILDCARD : (TERM_START_CHAR | '*' | '?') (TERM_CHAR | '*' | '?')* ;
REGEXP   : '/' (~[/] | '\\/')* '/' ;

LBRACKET : '[' -> pushMode(RANGE_MODE) ;
LBRACE   : '{' -> pushMode(RANGE_MODE) ;

IN_LPAREN  : [Ii][Nn] '(' -> pushMode(LIST_MODE) ;
ANY_LPAREN : [Aa][Nn][Yy] '(' -> pushMode(STRING_MODE) ;
ALL_LPAREN : [Aa][Ll][Ll] '(' -> pushMode(STRING_MODE) ;

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