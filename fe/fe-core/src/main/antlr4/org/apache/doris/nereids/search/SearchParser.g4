parser grammar SearchParser;

options { tokenVocab=SearchLexer; }

search     : clause EOF ;
clause     : orClause ;
orClause   : andClause (OR andClause)* ;
andClause  : notClause (AND notClause)* ;
notClause  : NOT atomClause | atomClause ;
atomClause : LPAREN clause RPAREN | fieldQuery ;
fieldQuery : fieldName COLON searchValue ;
fieldName  : TERM | QUOTED ;

searchValue
    : TERM
    | PREFIX
    | WILDCARD
    | REGEXP
    | QUOTED
    | rangeValue
    | listValue
    | anyAllValue
    ;

rangeValue
    : LBRACKET rangeEndpoint RANGE_TO rangeEndpoint RBRACKET
    | LBRACE rangeEndpoint RANGE_TO rangeEndpoint RBRACE
    ;

rangeEndpoint : RANGE_NUMBER | RANGE_STAR ;

listValue   : IN_LPAREN LIST_TERM* LIST_RPAREN ;
anyAllValue : (ANY_LPAREN | ALL_LPAREN) STRING_CONTENT? STRING_RPAREN ;