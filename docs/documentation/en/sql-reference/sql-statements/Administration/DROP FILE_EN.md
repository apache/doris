# DROP FILE
## Description

This statement is used to delete an uploaded file.

Grammar:

DROP FILE "file_name" [FROM database]
[properties]

Explain:
File_name: File name.
Database: A DB to which the file belongs, if not specified, uses the DB of the current session.
properties 支持以下参数:

Catalog: Yes. Classification of documents.

## example

1. Delete the file ca.pem

DROP FILE "ca.pem" properties("catalog" = "kafka");

## keyword
DROP,FILE
