---
layout: docs
title: Using Core C++
permalink: /docs/core-cpp.html
---

The C++ Core ORC API reads and writes ORC files into its own
orc::ColumnVectorBatch vectorized classes.

## Vectorized Row Batch

Data is passed to ORC as instances of orc::ColumnVectorBatch
that contain the data a batch of rows. The focus is on speed and
accessing the data fields directly. `numElements` is the number
of rows. ColumnVectorBatch is the parent type of the different
kinds of columns and has some fields that are shared across
all of the column types. In particular, the `hasNulls` flag
if there is any null in this column for this batch. For columns
where `hasNulls == true` the `notNull` buffer is false if that
value is null.

~~~ cpp
namespace orc {
  struct ColumnVectorBatch {
    uint64_t numElements;
    DataBuffer<char> notNull;
    bool hasNulls;
    ...
  }
}
~~~

The subtypes of ColumnVectorBatch are:

| ORC Type | ColumnVectorBatch |
| -------- | ------------- |
| array | ListVectorBatch |
| binary | StringVectorBatch |
| bigint | LongVectorBatch |
| boolean | LongVectorBatch |
| char | StringVectorBatch |
| date | LongVectorBatch |
| decimal | Decimal64VectorBatch, Decimal128VectorBatch |
| double | DoubleVectorBatch |
| float | DoubleVectorBatch |
| int | LongVectorBatch |
| map | MapVectorBatch |
| smallint | LongVectorBatch |
| string | StringVectorBatch |
| struct | StructVectorBatch |
| timestamp | TimestampVectorBatch |
| tinyint | LongVectorBatch |
| uniontype | UnionVectorBatch |
| varchar | StringVectorBatch |

LongVectorBatch handles all of the integer types (boolean, bigint,
date, int, smallint, and tinyint). The data is represented as a
buffer of int64_t where each value is sign-extended as necessary.

~~~ cpp
  struct LongVectorBatch: public ColumnVectorBatch {
    DataBuffer<int64_t> data;
    ...
  };
~~~

TimestampVectorBatch handles timestamp values. The data is
represented as two buffers of int64_t for seconds and nanoseconds
respectively. Note that we always assume data is in GMT timezone;
therefore it is user's responsibility to convert wall clock time
from local timezone to GMT.

~~~ cpp
  struct TimestampVectorBatch: public ColumnVectorBatch {
    DataBuffer<int64_t> data;
    DataBuffer<int64_t> nanoseconds;
    ...
  };
~~~

DoubleVectorBatch handles all of the floating point types
(double, and float). The data is represented as a buffer of doubles.

~~~ cpp
  struct DoubleVectorBatch: public ColumnVectorBatch {
    DataBuffer<double> data;
    ...
  };
~~~

Decimal64VectorBatch handles decimal columns with precision no
greater than 18. Decimal128VectorBatch handles the others. The data
is represented as a buffer of int64_t and orc::Int128 respectively.

~~~ cpp
  struct Decimal64VectorBatch: public ColumnVectorBatch {
    DataBuffer<int64_t> values;
    ...
  };

  struct Decimal128VectorBatch: public ColumnVectorBatch {
    DataBuffer<Int128> values;
    ...
  };
~~~

StringVectorBatch handles all of the binary types (binary,
char, string, and varchar). The data is represented as a char* buffer,
and a length buffer.

~~~ cpp
  struct StringVectorBatch: public ColumnVectorBatch {
    DataBuffer<char*> data;
    DataBuffer<int64_t> length;
    ...
  };
~~~

StructVectorBatch handles the struct columns and represents
the data as a buffer of `ColumnVectorBatch`.

~~~ cpp
  struct StructVectorBatch: public ColumnVectorBatch {
    std::vector<ColumnVectorBatch*> fields;
    ...
  };
~~~

UnionVectorBatch handles the union columns. It uses `tags`
to indicate which subtype has the value and `offsets` indicates
the offset in child batch of that subtype. A individual
`ColumnVectorBatch` is used for each subtype.

~~~ cpp
  struct UnionVectorBatch: public ColumnVectorBatch {
    DataBuffer<unsigned char> tags;
    DataBuffer<uint64_t> offsets;
    std::vector<ColumnVectorBatch*> children;
    ...
  };
~~~

ListVectorBatch handles the array columns and represents
the data as a buffer of integers for the offsets and a
`ColumnVectorBatch` for the children values.

~~~ cpp
  struct ListVectorBatch: public ColumnVectorBatch {
    DataBuffer<int64_t> offsets;
    std::unique_ptr<ColumnVectorBatch> elements;
    ...
  };
~~~

MapVectorBatch handles the map columns and represents the data
as two arrays of integers for the offsets and two `ColumnVectorBatch`s
for the keys and values.

~~~ cpp
  struct MapVectorBatch: public ColumnVectorBatch {
    DataBuffer<int64_t> offsets;
    std::unique_ptr<ColumnVectorBatch> keys;
    std::unique_ptr<ColumnVectorBatch> elements;
    ...
  };
~~~

## Writing ORC Files

To write an ORC file, you need to include `OrcFile.hh` and define
the schema; then use `orc::OutputStream` and `orc::WriterOptions`
to create a `orc::Writer` with the desired filename. This example
sets the required schema parameter, but there are many other
options to control the ORC writer.

~~~ cpp
std::unique_ptr<OutputStream> outStream =
  writeLocalFile("my-file.orc");
std::unique_ptr<Type> schema(
  Type::buildTypeFromString("struct<x:int,y:int>"));
WriterOptions options;
std::unique_ptr<Writer> writer =
  createWriter(*schema, outStream.get(), options);
~~~

Now you need to create a row batch, set the data, and write it to the file
as the batch fills up. When the file is done, close the `Writer`.

~~~ cpp
uint64_t batchSize = 1024, rowCount = 10000;
std::unique_ptr<ColumnVectorBatch> batch =
  writer->createRowBatch(batchSize);
StructVectorBatch *root =
  dynamic_cast<StructVectorBatch *>(batch.get());
LongVectorBatch *x =
  dynamic_cast<LongVectorBatch *>(root->fields[0]);
LongVectorBatch *y =
  dynamic_cast<LongVectorBatch *>(root->fields[1]);

uint64_t rows = 0;
for (uint64_t i = 0; i < rowCount; ++i) {
  x->data[rows] = i;
  y->data[rows] = i * 3;
  rows++;

  if (rows == batchSize) {
    root->numElements = rows;
    x->numElements = rows;
    y->numElements = rows;

    writer->add(*batch);
    rows = 0;
  }
}

if (rows != 0) {
  root->numElements = rows;
  x->numElements = rows;
  y->numElements = rows;

  writer->add(*batch);
  rows = 0;
}

writer->close();
~~~

## Reading ORC Files

To read ORC files, include `OrcFile.hh` file to create a `orc::Reader`
that contains the metadata about the file. There are a few options to
the `orc::Reader`, but far fewer than the writer and none of them are
required. The reader has methods for getting the number of rows,
schema, compression, etc. from the file.

~~~ cpp
std::unique_ptr<InputStream> inStream =
  readLocalFile("my-file.orc");
ReaderOptions options;
std::unique_ptr<Reader> reader =
  createReader(inStream, options);
~~~

To get the data, create a `orc::RowReader` object. By default,
the RowReader reads all rows and all columns, but there are
options to control the data that is read.

~~~ cpp
RowReaderOptions rowReaderOptions;
std::unique_ptr<RowReader> rowReader =
  reader->createRowReader(rowReaderOptions);
std::unique_ptr<ColumnVectorBatch> batch =
  rowReader->createRowBatch(1024);
~~~

With a `orc::RowReader` the user can ask for the next batch until there
are no more left. The reader will stop the batch at certain boundaries,
so the returned batch may not be full, but it will always contain some rows.

~~~ cpp
while (rowReader->next(*batch)) {
  for (uint64_t r = 0; r < batch->numElements; ++r) {
    ... process row r from batch
  }
}
~~~
