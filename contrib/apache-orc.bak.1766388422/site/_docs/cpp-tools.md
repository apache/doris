---
layout: docs
title: C++ Tools
permalink: /docs/cpp-tools.html
---

## orc-contents

Displays the contents of the ORC file as a JSON document. With the
`columns` argument only the selected columns are printed.

~~~ shell
% orc-contents [options] <filename>
Options:
	-h --help
	-c --columns		Comma separated list of top-level column fields
	-t --columnTypeIds	Comma separated list of column type ids
	-n --columnNames	Comma separated list of column names
	-b --batch		Batch size for reading
~~~

If you run it on the example file TestOrcFile.test1.orc, you'll see (without
the line breaks within each record):

~~~ shell
% orc-contents examples/TestOrcFile.test1.orc
{"boolean1": false, "byte1": 1, "short1": 1024, "int1": 65536, \\
 "long1": 9223372036854775807, "float1": 1, "double1": -15, \\
 "bytes1": [0, 1, 2, 3, 4], "string1": "hi", "middle": \\
    {"list": [{"int1": 1, "string1": "bye"}, \\
              {"int1": 2, "string1": "sigh"}]}, \\
 "list": [{"int1": 3, "string1": "good"}, \\
          {"int1": 4, "string1": "bad"}], \\
 "map": []}
{"boolean1": true, "byte1": 100, "short1": 2048, "int1": 65536,
 "long1": 9223372036854775807, "float1": 2, "double1": -5, \\
 "bytes1": [], "string1": "bye", \\
 "middle": {"list": [{"int1": 1, "string1": "bye"}, \\
                     {"int1": 2, "string1": "sigh"}]}, \\
 "list": [{"int1": 100000000, "string1": "cat"}, \\
          {"int1": -100000, "string1": "in"}, \\
          {"int1": 1234, "string1": "hat"}], \\
 "map": [{"key": "chani", "value": {"int1": 5, "string1": "chani"}}, \\
         {"key": "mauddib", \\
          "value": {"int1": 1, "string1": "mauddib"}}]}
~~~

## orc-metadata

Displays the metadata of the ORC file as a JSON document. With the
`verbose` option additional information about the layout of the file
is also printed.

For diagnosing problems, it is useful to use the '--raw' option that
prints the protocol buffers from the ORC file directly rather than
interpreting them.

~~~ shell
% orc-metadata [-v] [--raw] <filename>
~~~

If you run it on the example file TestOrcFile.test1.orc, you'll see:

~~~ shell
% orc-metadata examples/TestOrcFile.test1.orc
{ "name": "../examples/TestOrcFile.test1.orc",
  "type": "struct<boolean1:boolean,byte1:tinyint,short1:smallint,
int1:int,long1:bigint,float1:float,double1:double,bytes1:binary,
string1:string,middle:struct<list:array<struct<int1:int,string1:
string>>>,list:array<struct<int1:int,string1:string>>,map:map<
string,struct<int1:int,string1:string>>>",
  "rows": 2,
  "stripe count": 1,
  "format": "0.12", "writer version": "HIVE-8732",
  "compression": "zlib", "compression block": 10000,
  "file length": 1711,
  "content": 1015, "stripe stats": 250, "footer": 421, "postscript": 24,
  "row index stride": 10000,
  "user metadata": {
  },
  "stripes": [
    { "stripe": 0, "rows": 2,
      "offset": 3, "length": 1012,
      "index": 570, "data": 243, "footer": 199
    }
  ]
}
~~~

## csv-import

Imports CSV file into an Orc file using the specified schema.
Compound types are not yet supported. `delimiter` option indicates
the delimiter in the input CSV file and by default is `,`. `stripe`
option means the stripe size and set to 128MB by default. `block`
option is compression block size which is 64KB by default. `batch`
option is by default 1024 rows for one batch.

~~~ shell
% csv-import [--delimiter=<character>] [--stripe=<size>]
             [--block=<size>] [--batch=<size>]
             <schema> <inputCSVFile> <outputORCFile>
~~~

If you run it on the example file TestCSVFileImport.test10rows.csv,
you'll see:

~~~ shell
% csv-import "struct<a:bigint,b:string,c:double>"
             examples/TestCSVFileImport.test10rows.csv /tmp/test.orc
[2018-04-11 11:12:16] Start importing Orc file...
[2018-04-11 11:12:16] Finish importing Orc file.
[2018-04-11 11:12:16] Total writer elasped time: 0.001352s.
[2018-04-11 11:12:16] Total writer CPU time: 0.001339s.
~~~

## orc-scan

Scans and displays the row count of the ORC file. With the `batch` option
to set the batch size which is 1024 rows by default. It is useful to check
if the ORC file is damaged.

~~~ shell
% orc-scan [options] <filename>...
Options:
	-h --help
	-c --columns		Comma separated list of top-level column fields
	-t --columnTypeIds	Comma separated list of column type ids
	-n --columnNames	Comma separated list of column names
	-b --batch		Batch size for reading
~~~

If you run it on the example file TestOrcFile.test1.orc, you'll see:

~~~ shell
% orc-scan examples/TestOrcFile.test1.orc
Rows: 2
Batches: 1
~~~

## orc-statistics

Displays the file-level and stripe-level column statistics of the ORC file.
With the `withIndex` option to include column statistics in each row group.

~~~ shell
% orc-statistics [--withIndex] <filename>
~~~

If you run it on the example file TestOrcFile.columnProjection.orc
you'll see:

~~~ shell
% orc-statistics examples/TestOrcFile.columnProjection.orc
File examples/TestOrcFile.columnProjection.orc has 3 columns
*** Column 0 ***
Column has 21000 values and has null value: no

*** Column 1 ***
Data type: Integer
Values: 21000
Has null: no
Minimum: -2147439072
Maximum: 2147257982
Sum: 268482658568

*** Column 2 ***
Data type: String
Values: 21000
Has null: no
Minimum: 100119c272d7db89
Maximum: fffe9f6f23b287f3
Total length: 334559

File examples/TestOrcFile.columnProjection.orc has 5 stripes
*** Stripe 0 ***

--- Column 0 ---
Column has 5000 values and has null value: no

--- Column 1 ---
Data type: Integer
Values: 5000
Has null: no
Minimum: -2145365268
Maximum: 2147025027
Sum: -29841423854

--- Column 2 ---
Data type: String
Values: 5000
Has null: no
Minimum: 1005350489418be2
Maximum: fffbb8718c92b09f
Total length: 79644

*** Stripe 1 ***

--- Column 0 ---
Column has 5000 values and has null value: no

--- Column 1 ---
Data type: Integer
Values: 5000
Has null: no
Minimum: -2147115959
Maximum: 2147257982
Sum: 108604887785

--- Column 2 ---
Data type: String
Values: 5000
Has null: no
Minimum: 100119c272d7db89
Maximum: fff0ae41d41e6afc
Total length: 79640

*** Stripe 2 ***

--- Column 0 ---
Column has 5000 values and has null value: no

--- Column 1 ---
Data type: Integer
Values: 5000
Has null: no
Minimum: -2145932387
Maximum: 2145877119
Sum: 70064190848

--- Column 2 ---
Data type: String
Values: 5000
Has null: no
Minimum: 10130af874ae036c
Maximum: fffe9f6f23b287f3
Total length: 79645

*** Stripe 3 ***

--- Column 0 ---
Column has 5000 values and has null value: no

--- Column 1 ---
Data type: Integer
Values: 5000
Has null: no
Minimum: -2147439072
Maximum: 2147074354
Sum: 104681356482

--- Column 2 ---
Data type: String
Values: 5000
Has null: no
Minimum: 102547d48ed06518
Maximum: fffa47c57dc7b69a
Total length: 79689

*** Stripe 4 ***

--- Column 0 ---
Column has 1000 values and has null value: no

--- Column 1 ---
Data type: Integer
Values: 1000
Has null: no
Minimum: -2141222223
Maximum: 2145816096
Sum: 14973647307

--- Column 2 ---
Data type: String
Values: 1000
Has null: no
Minimum: 1059d81c9025a217
Maximum: ffc17f0e35e1a6c0
Total length: 15941
~~~

## orc-memory

Estimate the memory footprint for reading the ORC file.

~~~ shell
% orc-memory [options] <filename>
Options:
	-h --help
	-c --columns		Comma separated list of top-level column fields
	-t --columnTypeIds	Comma separated list of column type ids
	-n --columnNames	Comma separated list of column names
	-b --batch		Batch size for reading
~~~

If you run it on the example file TestOrcFile.columnProjection.orc
you'll see:

~~~ shell
% orc-memory examples/TestOrcFile.columnProjection.orc,
Reader memory estimate: 202972
Batch memory estimate:  27000
Total memory estimate:  229972
Actual max memory used: 160381
~~~
