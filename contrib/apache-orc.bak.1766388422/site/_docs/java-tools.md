---
layout: docs
title: Java Tools
permalink: /docs/java-tools.html
---

In addition to the C++ tools, there is an ORC tools jar that packages
several useful utilities and the necessary Java dependencies
(including Hadoop) into a single package. The Java ORC tool jar
supports both the local file system and HDFS.

The subcommands for the tools are:

  * convert (since ORC 1.4) - convert JSON/CSV files to ORC
  * count (since ORC 1.6) - recursively find *.orc and print the number of rows
  * data - print the data of an ORC file
  * json-schema (since ORC 1.4) - determine the schema of JSON documents
  * key (since ORC 1.5) - print information about the encryption keys
  * meta - print the metadata of an ORC file
  * scan (since ORC 1.3) - scan the data for benchmarking
  * sizes (since ORC 1.7.2) - list size on disk of each column
  * version (since ORC 1.6) - print the version of this ORC tool

The command line looks like:

~~~ shell
% java -jar orc-tools-X.Y.Z-uber.jar <sub-command> <args>
~~~

## Java Convert

The convert command reads several JSON/CSV files and converts them into a
single ORC file.

`-b,--bloomFilterColumns <columns>`
  : Comma separated values of column names for which bloom filter is to be created.
  By default, no bloom filters will be created.

`-e,--escape <escape>`
  : Sets CSV escape character

`-h,--help`
  : Print help

`-H,--header <header>`
  : Sets CSV header lines

`-n,--null <null>`
  : Sets CSV null string

`-o,--output <filename>`
  : Sets the output ORC filename, which defaults to output.orc

`-O,--overwrite`
  : If the file already exists, it will be overwritten

`-q,--quote <quote>`
  : Sets CSV quote character

`-s,--schema <schema>`
  : Sets the schema for the ORC file. By default, the schema is automatically discovered.

`-S,--separator <separator>`
  : Sets CSV separator character

`-t,--timestampformat <timestampformat>`
  : Sets timestamp Format
  
The automatic JSON schema discovery is equivalent to the json-schema tool
below.

## Java Count

The count command recursively find *.orc and print the number of rows. 

## Java Data

The data command prints the data in an ORC file as a JSON document. Each
record is printed as a JSON object on a line. Each record is annotated with
the fieldnames and a JSON representation that depends on the field's type.

`-h,--help`
   : Print help

`-n,--lines <LINES>`
   : Sets lines of data to be printed 

## Java JSON Schema

The JSON Schema discovery tool processes a set of JSON documents and
produces a schema that encompasses all of the records in all of the
documents. It works by computing the enclosing type and promoting it
to include all of the observed values.

`-f,--flat`
  : Print the schema as a list of flat types for each subfield

`-h,--help`
  : Print help

`-p,--pretty`
  : Pretty print the schema

`-t,--table`
  : Print the schema as a Hive table declaration  

## Java Key

The key command prints the information about the encryption keys.

`-h,--help`
  : Print help

`-o,--output <output>`
  : Output filename

## Java Meta

The meta command prints the metadata about the given ORC file and is
equivalent to the Hive ORC File Dump command.

`--backup-path <path>`
  : when used with --recover specifies the path where the recovered file is written (default: /tmp)

`-d,--data`
  : Should the data be printed

`-h,--help`
  : Print help

`-j,--json`
  : Format the output in JSON

`-p,--pretty`
  : Pretty print the output

`-r,--rowindex <ids>`
  : Print the row indexes for the comma separated list of column ids

`--recover`
  : Skip over corrupted values in the ORC file

`--skip-dump`
  : Skip dumping the metadata

`-t,--timezone`
  : Print the timezone of the writer


An example of the output is given below:

~~~ shell
% java -jar orc-tools-X.Y.Z-uber.jar meta examples/TestOrcFile.test1.orc
Processing data file examples/TestOrcFile.test1.orc [length: 1711]
Structure for examples/TestOrcFile.test1.orc
File Version: 0.12 with HIVE_8732
Rows: 2
Compression: ZLIB
Compression size: 10000
Type: struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,
long1:bigint,float1:float,double1:double,bytes1:binary,string1:string,
middle:struct<list:array<struct<int1:int,string1:string>>>,list:array<
struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:
string>>>

Stripe Statistics:
  Stripe 1:
    Column 0: count: 2 hasNull: false
    Column 1: count: 2 hasNull: false true: 1
    Column 2: count: 2 hasNull: false min: 1 max: 100 sum: 101
    Column 3: count: 2 hasNull: false min: 1024 max: 2048 sum: 3072
    Column 4: count: 2 hasNull: false min: 65536 max: 65536 sum: 131072
    Column 5: count: 2 hasNull: false min: 9223372036854775807 max: 9223372036854775807
    Column 6: count: 2 hasNull: false min: 1.0 max: 2.0 sum: 3.0
    Column 7: count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0
    Column 8: count: 2 hasNull: false sum: 5
    Column 9: count: 2 hasNull: false min: bye max: hi sum: 5
    Column 10: count: 2 hasNull: false
    Column 11: count: 2 hasNull: false
    Column 12: count: 4 hasNull: false
    Column 13: count: 4 hasNull: false min: 1 max: 2 sum: 6
    Column 14: count: 4 hasNull: false min: bye max: sigh sum: 14
    Column 15: count: 2 hasNull: false
    Column 16: count: 5 hasNull: false
    Column 17: count: 5 hasNull: false min: -100000 max: 100000000 sum: 99901241
    Column 18: count: 5 hasNull: false min: bad max: in sum: 15
    Column 19: count: 2 hasNull: false
    Column 20: count: 2 hasNull: false min: chani max: mauddib sum: 12
    Column 21: count: 2 hasNull: false
    Column 22: count: 2 hasNull: false min: 1 max: 5 sum: 6
    Column 23: count: 2 hasNull: false min: chani max: mauddib sum: 12

File Statistics:
  Column 0: count: 2 hasNull: false
  Column 1: count: 2 hasNull: false true: 1
  Column 2: count: 2 hasNull: false min: 1 max: 100 sum: 101
  Column 3: count: 2 hasNull: false min: 1024 max: 2048 sum: 3072
  Column 4: count: 2 hasNull: false min: 65536 max: 65536 sum: 131072
  Column 5: count: 2 hasNull: false min: 9223372036854775807 max: 9223372036854775807
  Column 6: count: 2 hasNull: false min: 1.0 max: 2.0 sum: 3.0
  Column 7: count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0
  Column 8: count: 2 hasNull: false sum: 5
  Column 9: count: 2 hasNull: false min: bye max: hi sum: 5
  Column 10: count: 2 hasNull: false
  Column 11: count: 2 hasNull: false
  Column 12: count: 4 hasNull: false
  Column 13: count: 4 hasNull: false min: 1 max: 2 sum: 6
  Column 14: count: 4 hasNull: false min: bye max: sigh sum: 14
  Column 15: count: 2 hasNull: false
  Column 16: count: 5 hasNull: false
  Column 17: count: 5 hasNull: false min: -100000 max: 100000000 sum: 99901241
  Column 18: count: 5 hasNull: false min: bad max: in sum: 15
  Column 19: count: 2 hasNull: false
  Column 20: count: 2 hasNull: false min: chani max: mauddib sum: 12
  Column 21: count: 2 hasNull: false
  Column 22: count: 2 hasNull: false min: 1 max: 5 sum: 6
  Column 23: count: 2 hasNull: false min: chani max: mauddib sum: 12

Stripes:
  Stripe: offset: 3 data: 243 rows: 2 tail: 199 index: 570
    Stream: column 0 section ROW_INDEX start: 3 length 11
    Stream: column 1 section ROW_INDEX start: 14 length 22
    Stream: column 2 section ROW_INDEX start: 36 length 26
    Stream: column 3 section ROW_INDEX start: 62 length 27
    Stream: column 4 section ROW_INDEX start: 89 length 30
    Stream: column 5 section ROW_INDEX start: 119 length 28
    Stream: column 6 section ROW_INDEX start: 147 length 34
    Stream: column 7 section ROW_INDEX start: 181 length 34
    Stream: column 8 section ROW_INDEX start: 215 length 21
    Stream: column 9 section ROW_INDEX start: 236 length 30
    Stream: column 10 section ROW_INDEX start: 266 length 11
    Stream: column 11 section ROW_INDEX start: 277 length 16
    Stream: column 12 section ROW_INDEX start: 293 length 11
    Stream: column 13 section ROW_INDEX start: 304 length 24
    Stream: column 14 section ROW_INDEX start: 328 length 31
    Stream: column 15 section ROW_INDEX start: 359 length 16
    Stream: column 16 section ROW_INDEX start: 375 length 11
    Stream: column 17 section ROW_INDEX start: 386 length 32
    Stream: column 18 section ROW_INDEX start: 418 length 30
    Stream: column 19 section ROW_INDEX start: 448 length 16
    Stream: column 20 section ROW_INDEX start: 464 length 37
    Stream: column 21 section ROW_INDEX start: 501 length 11
    Stream: column 22 section ROW_INDEX start: 512 length 24
    Stream: column 23 section ROW_INDEX start: 536 length 37
    Stream: column 1 section DATA start: 573 length 5
    Stream: column 2 section DATA start: 578 length 6
    Stream: column 3 section DATA start: 584 length 9
    Stream: column 4 section DATA start: 593 length 11
    Stream: column 5 section DATA start: 604 length 12
    Stream: column 6 section DATA start: 616 length 11
    Stream: column 7 section DATA start: 627 length 15
    Stream: column 8 section DATA start: 642 length 8
    Stream: column 8 section LENGTH start: 650 length 6
    Stream: column 9 section DATA start: 656 length 8
    Stream: column 9 section LENGTH start: 664 length 6
    Stream: column 11 section LENGTH start: 670 length 6
    Stream: column 13 section DATA start: 676 length 7
    Stream: column 14 section DATA start: 683 length 6
    Stream: column 14 section LENGTH start: 689 length 6
    Stream: column 14 section DICTIONARY_DATA start: 695 length 10
    Stream: column 15 section LENGTH start: 705 length 6
    Stream: column 17 section DATA start: 711 length 25
    Stream: column 18 section DATA start: 736 length 18
    Stream: column 18 section LENGTH start: 754 length 8
    Stream: column 19 section LENGTH start: 762 length 6
    Stream: column 20 section DATA start: 768 length 15
    Stream: column 20 section LENGTH start: 783 length 6
    Stream: column 22 section DATA start: 789 length 6
    Stream: column 23 section DATA start: 795 length 15
    Stream: column 23 section LENGTH start: 810 length 6
    Encoding column 0: DIRECT
    Encoding column 1: DIRECT
    Encoding column 2: DIRECT
    Encoding column 3: DIRECT_V2
    Encoding column 4: DIRECT_V2
    Encoding column 5: DIRECT_V2
    Encoding column 6: DIRECT
    Encoding column 7: DIRECT
    Encoding column 8: DIRECT_V2
    Encoding column 9: DIRECT_V2
    Encoding column 10: DIRECT
    Encoding column 11: DIRECT_V2
    Encoding column 12: DIRECT
    Encoding column 13: DIRECT_V2
    Encoding column 14: DICTIONARY_V2[2]
    Encoding column 15: DIRECT_V2
    Encoding column 16: DIRECT
    Encoding column 17: DIRECT_V2
    Encoding column 18: DIRECT_V2
    Encoding column 19: DIRECT_V2
    Encoding column 20: DIRECT_V2
    Encoding column 21: DIRECT
    Encoding column 22: DIRECT_V2
    Encoding column 23: DIRECT_V2

File length: 1711 bytes
Padding length: 0 bytes
Padding ratio: 0%
______________________________________________________________________
~~~

## Java Scan

The scan command reads the contents of the file without printing anything. It
is primarily intendend for benchmarking the Java reader without including the
cost of printing the data out.

`-h,--help`
  : Print help

`-s,--schema`
  : Print schema

`-v,--verbose`
  : Print exceptions

## Java Sizes

The sizes command lists size on disk of each column. The output contains not
only the raw data of the table, but also the size of metadata such as `padding`,
`stripeFooter`, `fileFooter`, `stripeIndex` and `stripeData`.

~~~ shell
% java -jar orc-tools-X.Y.Z-uber.jar sizes examples/my-file.orc
Percent  Bytes/Row  Name
  98.45  2.62       y
  0.81   0.02       _file_footer
  0.30   0.01       _index
  0.25   0.01       x
  0.19   0.01       _stripe_footer
______________________________________________________________________
~~~

## Java Version

The version command prints the version of this ORC tool.
