# SHOW TABLE STATUS

## description

This statement is used to view some information about Table.

    Syntax:

    SHOW TABLE STATUS
    [FROM db] [LIKE "pattern"]

    Explain:

    1. This statement is mainly used to be compatible with MySQL grammar. At present, only a small amount of information such as Comment is displayed.

## Example

    1. View the information of all tables under the current database
    
        SHOW TABLE STATUS;
    
    
    2. View the information of the table whose name contains example in the specified database
    
        SHOW TABLE STATUS FROM DB LIKE "% example%";

## Keyword

    SHOW,TABLE,STATUS