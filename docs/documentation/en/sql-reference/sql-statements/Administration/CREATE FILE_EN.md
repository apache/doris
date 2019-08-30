# CREATE FILE
## Description

This statement is used to create and upload a file to the Doris cluster.
This function is usually used to manage files that need to be used in some other commands, such as certificates, public key, private key, etc.

This command can be executed by users with amdin privileges only.
A file belongs to a database. This file can be used by users who have access to database.

The size of a single file is limited to 1MB.
A Doris cluster uploads up to 100 files.

Grammar:

CREATE FILE "File name" [IN database]
[properties]

Explain:
File_name: Custom file name.
Database: The file belongs to a db, and if not specified, the DB of the current session is used.
properties 支持以下参数:

Url: Must. Specify a download path for a file. Currently only unauthenticated HTTP download paths are supported. When the command line succeeds, the file will be saved in Doris and the URL will no longer be required.
Catalog: Yes. The classification name of the file can be customized. But in some commands, files in the specified catalog are looked up. For example, in a routine import, when the data source is kafka, the file under the name of catalog is looked up.
Md5: Optional. MD5 of the file. If specified, it will be checked after downloading the file.

## example

1. Create a file ca. pem, categorized as Kafka

CREATE FILE "ca.pem"
PROPERTIES
(
"url" ="https://test.bj.bcebos.com /kafka -key /ca.pem",
"catalog" = "kafka"
);

2. Create the file client. key, categorized as my_catalog

CREATE FILE "client.key"
IN my database
PROPERTIES
(
"url" ="https://test.bj.bcebos.com /kafka -key /client.key",
"catalog" = "my_catalog",
"md5"= "b5bb901bf1099205b39a46ac3557dd9"
);

## keyword
CREATE,FILE
