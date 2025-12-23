---
layout: docs
title: Using Core Java
permalink: /docs/core-java.html
---

The Core ORC API reads and writes ORC files into Hive's storage-api
vectorized classes. Both Hive and MapReduce use the Core API to actually
read and write the data.

## Vectorized Row Batch

Data is passed to ORC as instances of
[VectorizedRowBatch](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/VectorizedRowBatch.html)
that contain the data for 1024 rows. The focus is on speed and
accessing the data fields directly. `cols` is an array of
[ColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/ColumnVector.html)
and `size` is the number of rows.

~~~ java
package org.apache.hadoop.hive.ql.exec.vector;

public class VectorizedRowBatch {
  public ColumnVector[] cols;
  public int size;
  ...
}
~~~

[ColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/ColumnVector.html)
is the parent type of the different kinds of columns and has some
fields that are shared across all of the column types. In particular,
the `noNulls` flag if there are no nulls in this column for this batch
and the `isRepeating` flag for columns were the entire batch is the
same value. For columns where `noNulls == false` the `isNull` array is
true if that value is null.

~~~ java
public abstract class ColumnVector {

  // If the whole column vector has no nulls, this is true, otherwise false.
  public boolean noNulls;

  // If hasNulls is true, then this array contains true if the value is
  // is null, otherwise false.
  public boolean[] isNull;

  /*
   * True if same value repeats for whole column vector.
   * If so, vector[0] holds the repeating value.
   */
  public boolean isRepeating;
  ...
}
~~~

The subtypes of ColumnVector are:

| ORC Type | ColumnVector |
| -------- | ------------- |
| array | [ListColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/ListColumnVector.html) |
| binary | [BytesColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/BytesColumnVector.html) |
| bigint | [LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) |
| boolean | [LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) |
| char | [BytesColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/BytesColumnVector.html) |
| date | [LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) |
| decimal | [DecimalColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/DecimalColumnVector.html) |
| double | [DoubleColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/DoubleColumnVector.html) |
| float | [DoubleColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/DoubleColumnVector.html) |
| int | [LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) |
| map | [MapColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/MapColumnVector.html) |
| smallint | [LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) |
| string | [BytesColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/BytesColumnVector.html) |
| struct | [StructColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/StructColumnVector.html) |
| timestamp | [TimestampColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/TimestampColumnVector.html) |
| tinyint | [LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) |
| uniontype | [UnionColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/UnionColumnVector.html) |
| varchar | [BytesColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/BytesColumnVector.html) |

[LongColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/LongColumnVector.html) handles all of the integer types (boolean, bigint,
date, int, smallint, and tinyint). The data is represented as an array of
longs where each value is sign-extended as necessary.

~~~ java
public class LongColumnVector extends ColumnVector {
  public long[] vector;
  ...
}
~~~

[TimestampColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/TimestampColumnVector.html)
handles timestamp values. The data is represented as an array of longs
and an array of ints.

~~~ java
public class TimestampColumnVector extends ColumnVector {

  // the number of milliseconds since 1 Jan 1970 00:00 GMT
  public long[] time;

  // the number of nanoseconds within the second
  public int[] nanos
  ...
}
~~~

[DoubleColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/DoubleColumnVector.html)
handles all of the floating point types (double, and float). The data
is represented as an array of doubles.

~~~ java
public class DoubleColumnVector extends ColumnVector {
  public double[] vector;
  ...
}
~~~

[DecimalColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/DecimalColumnVector.html)
handles decimal columns. The data is represented as an array of
HiveDecimalWritable. Note that this implementation is not performant
and will likely be replaced.

~~~ java
public class DecimalColumnVector extends ColumnVector {
  public HiveDecimalWritable[] vector;
  ...
}
~~~

[BytesColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/BytesColumnVector.html)
handles all of the binary types (binary, char, string, and
varchar). The data is represented as a byte array, offset, and
length. The byte arrays may or may not be shared between values.

~~~ java
public class BytesColumnVector extends ColumnVector {
  public byte[][] vector;
  public int[] start;
  public int[] length;
  ...
}
~~~

[StructColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/StructColumnVector.html)
handles the struct columns and represents the data as an array of
`ColumnVector`. The value for row 5 consists of the fifth value from
each of the `fields` values.

~~~ java
public class StructColumnVector extends ColumnVector {
  public ColumnVector[] fields;
  ...
}
~~~

[UnionColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/UnionColumnVector.html)
handles the union columns and represents the data as an array of
integers that pick the subtype and a `fields` array one per a
subtype. Only the value of the `fields` that corresponds to
`tags[row]` is set.

~~~ java
public class UnionColumnVector extends ColumnVector {
  public int[] tags;
  public ColumnVector[] fields;
  ...
}
~~~

[ListColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/ListColumnVector.html)
handles the array columns and represents the data as two arrays of
integers for the offset and lengths and a `ColumnVector` for the
children values.

~~~ java
public class ListColumnVector extends ColumnVector {
  // for each row, the first offset of the child
  public long[] offsets;
  // for each row, the number of elements in the array
  public long[] lengths;
  // the offset in the child that should be used for new values
  public int childCount;

  // the values of the children
  public ColumnVector child;
  ...
}
~~~

[MapColumnVector](/api/hive-storage-api/index.html?org/apache/hadoop/hive/ql/exec/vector/MapColumnVector.html)
handles the map columns and represents the data as two arrays of
integers for the offset and lengths and two `ColumnVector`s for the
keys and values.

~~~ java
public class MapColumnVector extends ColumnVector {
  // for each row, the first offset of the child
  public long[] offsets;
  // for each row, the number of elements in the array
  public long[] lengths;
  // the offset in the child that should be used for new values
  public int childCount;

  // the values of the keys and values
  public ColumnVector keys;
  public ColumnVector values;
  ...
}
~~~

## Writing ORC Files

### Simple Example
To write an ORC file, you need to define the schema and use the
[OrcFile](/api/orc-core/index.html?org/apache/orc/OrcFile.html)
class to create a
[Writer](/api/orc-core/index.html?org/apache/orc/Writer.html)
with the desired filename. This example sets the required schema
parameter, but there are many other options to control the ORC writer.

~~~ java
Configuration conf = new Configuration();
TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
Writer writer = OrcFile.createWriter(new Path("my-file.orc"),
                  OrcFile.writerOptions(conf)
                         .setSchema(schema));
~~~

Now you need to create a row batch, set the data, and write it to the file
as the batch fills up. When the file is done, close the `Writer`.

~~~ java
VectorizedRowBatch batch = schema.createRowBatch();
LongColumnVector x = (LongColumnVector) batch.cols[0];
LongColumnVector y = (LongColumnVector) batch.cols[1];
for(int r=0; r < 10000; ++r) {
  int row = batch.size++;
  x.vector[row] = r;
  y.vector[row] = r * 3;
  // If the batch is full, write it out and start over.
  if (batch.size == batch.getMaxSize()) {
    writer.addRowBatch(batch);
    batch.reset();
  }
}
if (batch.size != 0) {
  writer.addRowBatch(batch);
  batch.reset();
}
writer.close();
~~~

### Advanced Example

The following example writes an ORC file with two integer
columns and a map column. Each row's map has 5 elements with keys
ranging from "&lt;row&gt;.0" to "&lt;row&gt;.4".

~~~ java
Path testFilePath = new Path("advanced-example.orc");
Configuration conf = new Configuration();

TypeDescription schema =
    TypeDescription.fromString("struct<first:int," +
                               "second:int,third:map<string,int>>");

Writer writer =
    OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema));

VectorizedRowBatch batch = schema.createRowBatch();
LongColumnVector first = (LongColumnVector) batch.cols[0];
LongColumnVector second = (LongColumnVector) batch.cols[1];

//Define map. You need also to cast the key and value vectors
MapColumnVector map = (MapColumnVector) batch.cols[2];
BytesColumnVector mapKey = (BytesColumnVector) map.keys;
LongColumnVector mapValue = (LongColumnVector) map.values;

// Each map has 5 elements
final int MAP_SIZE = 5;
final int BATCH_SIZE = batch.getMaxSize();

// Ensure the map is big enough
mapKey.ensureSize(BATCH_SIZE * MAP_SIZE, false);
mapValue.ensureSize(BATCH_SIZE * MAP_SIZE, false);

// add 1500 rows to file
for(int r=0; r < 1500; ++r) {
  int row = batch.size++;

  first.vector[row] = r;
  second.vector[row] = r * 3;

  map.offsets[row] = map.childCount;
  map.lengths[row] = MAP_SIZE;
  map.childCount += MAP_SIZE;

  for (int mapElem = (int) map.offsets[row];
       mapElem < map.offsets[row] + MAP_SIZE; ++mapElem) {
    String key = "row " + r + "." + (mapElem - map.offsets[row]);
    mapKey.setVal(mapElem, key.getBytes(StandardCharsets.UTF_8));
    mapValue.vector[mapElem] = mapElem;
  }
  if (row == BATCH_SIZE - 1) {
    writer.addRowBatch(batch);
    batch.reset();
  }
}
if (batch.size != 0) {
  writer.addRowBatch(batch);
  batch.reset();
}
writer.close();

~~~


## Reading ORC Files

To read ORC files, use the
[OrcFile](/api/orc-core/index.html?org/apache/orc/OrcFile.html)
class to create a
[Reader](/api/orc-core/index.html?org/apache/orc/Reader.html)
that contains the metadata about the file. There are a few options to
the ORC reader, but far fewer than the writer and none of them are
required. The reader has methods for getting the number of rows,
schema, compression, etc. from the file.

~~~ java
Reader reader = OrcFile.createReader(new Path("my-file.orc"),
                  OrcFile.readerOptions(conf));
~~~

To get the data, create a
[RecordReader](/api/orc-core/index.html?org/apache/orc/RecordReader.html)
object. By default, the RecordReader reads all rows and all columns,
but there are options to control the data that is read.

~~~ java
RecordReader rows = reader.rows();
VectorizedRowBatch batch = reader.getSchema().createRowBatch();
~~~

With a `RecordReader` the user can ask for the next batch until there
are no more left. The reader will stop the batch at certain boundaries, so the
returned batch may not be full, but it will always contain some rows.

~~~ java
while (rows.nextBatch(batch)) {
  for(int r=0; r < batch.size; ++r) {
    ... process row r from batch
  }
}
rows.close();
~~~
