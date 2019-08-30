# SHOW FILE
## Description

This statement is used to show a file created in a database

Grammar:

SHOW FILE [FROM database];

Explain:

FileId: File ID, globally unique
DbName: The name of the database to which it belongs
Catalog: Custom Categories
FileName: File name
FileSize: File size, unit byte
MD5: Document MD5

## example

1. View uploaded files in my_database

SHOW FILE FROM my_database;

## keyword
SHOW,FILE
