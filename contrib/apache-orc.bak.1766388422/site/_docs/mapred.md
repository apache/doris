---
layout: docs
title: Using in MapRed
permalink: /docs/mapred.html
---

This page describes how to read and write ORC files from Hadoop's
older org.apache.hadoop.mapred MapReduce APIs. If you want to use the
new org.apache.hadoop.mapreduce API, please look at the [next
page](/docs/mapreduce.html).

## Reading ORC files

Add ORC and your desired version of Hadoop to your `pom.xml`:

~~~ xml
<dependencies>
  <dependency>
    <groupId>org.apache.orc</groupId>
    <artifactId>orc-mapreduce</artifactId>
    <version>1.1.0</version>
  </dependency>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-mapreduce-client-core</artifactId>
    <version>2.7.0</version>
  </dependency>
</dependencies>
~~~

Set the minimal properties in your JobConf:

* **mapreduce.job.inputformat.class** = [org.apache.orc.mapred.OrcInputFormat](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcInputFormat.html)
* **mapreduce.input.fileinputformat.inputdir** = your input directory

ORC files contain a series of values of the same type and that type
schema is encoded in the file. Because the ORC files are
self-describing, the reader always knows how to correctly interpret
the data. All of the ORC files written by Hive and most of the others have
a struct as the value type.

Your Mapper class will receive org.apache.hadoop.io.NullWritable as
the key and a value based on the table below expanded recursively.

| ORC Type | Writable Type |
| -------- | ------------- |
| array | [org.apache.orc.mapred.OrcList](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcStruct.html) |
| binary | org.apache.hadoop.io.BytesWritable |
| bigint | org.apache.hadoop.io.LongWritable |
| boolean | org.apache.hadoop.io.BooleanWritable |
| char | org.apache.hadoop.io.Text |
| date | [org.apache.hadoop.hive.serde2.io.DateWritable](/api/hive-storage-api/index.html?org/apache/hadoop/hive/serde2/io/DateWritable.html) |
| decimal | [org.apache.hadoop.hive.serde2.io.HiveDecimalWritable](/api/hive-storage-api/index.html?org/apache/hadoop/hive/serde2/io/HiveDecimalWritable.html) |
| double | org.apache.hadoop.io.DoubleWritable |
| float | org.apache.hadoop.io.FloatWritable |
| int | org.apache.hadoop.io.IntWritable |
| map | [org.apache.orc.mapred.OrcMap](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcMap.html) |
| smallint | org.apache.hadoop.io.ShortWritable |
| string | org.apache.hadoop.io.Text |
| struct | [org.apache.orc.mapred.OrcStruct](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcStruct.html) |
| timestamp | [org.apache.orc.mapred.OrcTimestamp](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcTimestamp.html) |
| tinyint | org.apache.hadoop.io.ByteWritable |
| uniontype | [org.apache.orc.mapred.OrcUnion](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcUnion.html) |
| varchar | org.apache.hadoop.io.Text |

Let's assume that your input directory contains ORC files with the
schema `struct<s:string,i:int>` and you want to use the string field
as the key to the MapReduce shuffle and the integer as the value. The
mapper code would look like:

~~~ java
public class MyMapper
    implements Mapper<NullWritable,OrcStruct,Text,IntWritable> {

  // Input should be: struct<s:string,i:int>
  public void map(NullWritable key, OrcStruct value,
                  OutputCollector<Text,IntWritable> output,
                  Reporter reporter) throws IOException {
    output.collect((Text) value.getFieldValue(0),
                   (IntWritable) value.getFieldValue(1));
  }

  public void configure(JobConf conf) { }

  public void close() { }
}
~~~

## Writing ORC files

To write ORC files from your MapReduce job, you'll need to set

* **mapreduce.job.outputformat.class** = [org.apache.orc.mapred.OrcOutputFormat](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcOutputFormat.html)
* **mapreduce.output.fileoutputformat.outputdir** = your output directory
* **orc.mapred.output.schema** = the schema to write to the ORC file

The reducer needs to create the Writable value to be put into the ORC
file and typically uses the OrcStruct.createValue(TypeDescription)
function. For our example, let's assume that the shuffle types are
(Text, IntWritable) from the previous section and the reduce should
gather the integer for each key together and write them as a list. The
output schema would be `struct<key:string,ints:array<int>>`. As always
with MapReduce, if your method stores the values, you need to copy their
value before getting the next.

~~~ java
public static class MyReducer
  implements Reducer<Text,IntWritable,NullWritable,OrcStruct> {

  private TypeDescription schema =
    TypeDescription.fromString("struct<key:string,ints:array<int>>");
  // createValue creates the correct value type for the schema
  private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
  // get a handle to the list of ints
  private OrcList<IntWritable> values =
    (OrcList<IntWritable>) pair.getFieldValue(1);
  private final NullWritable nada = NullWritable.get();

  public void reduce(Text key, Iterator<IntWritable> iterator,
                     OutputCollector<NullWritable, OrcStruct> output,
                     Reporter reporter) throws IOException {
    pair.setFieldValue(0, key);
    values.clear();
    while (iterator.hasNext()) {
      values.add(new IntWritable(iterator.next().get()));
    }
    output.collect(nada, pair);
  }

  public void configure(JobConf conf) { }

  public void close() { }
}
~~~

## Sending OrcStruct, OrcList, OrcMap, or OrcUnion through the Shuffle

In the previous examples, only the Hadoop types were sent through the
MapReduce shuffle. The complex ORC types, since they are generic
types, need to have their full type information provided to create the
object. To enable MapReduce to properly instantiate the OrcStruct and
other ORC types, we need to wrap it in either an
[OrcKey](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcKey.html)
for the shuffle key or
[OrcValue](/api/orc-mapreduce/index.html?org/apache/orc/mapred/OrcValue.html)
for the shuffle value.

To send two OrcStructs through the shuffle, define the following properties
in the JobConf:

* **mapreduce.map.output.key.class** = org.apache.orc.mapred.OrcKey
* **orc.mapred.map.output.key.schema** = the shuffle key's schema
* **mapreduce.map.output.value.class** = org.apache.orc.mapred.OrcValue
* **orc.mapred.map.output.value.schema** = the shuffle value's schema

The mapper just adds an OrcKey and OrcWrapper around the key and value
respectively. These objects should be created once and reused as the mapper
runs.

~~~ java
public static class MyMapperWithShuffle
  implements Mapper<NullWritable,OrcStruct,OrcKey,OrcValue> {

  // create wrapper objects
  private OrcKey keyWrapper = new OrcKey();
  private OrcValue valueWrapper = new OrcValue();

  // create a new structure to pass as the value in the shuffle
  private OrcStruct outStruct = (OrcStruct) OrcStruct.createValue
    (TypeDescription.fromString("struct<i:int,j:int>"));

  // get the two fields of the outStruct
  private IntWritable i = (IntWritable) outStruct.getFieldValue("i");
  private IntWritable j = (IntWritable) outStruct.getFieldValue("j");

  // Assume the input has type: struct<s:string,i:int>
  public void map(NullWritable key, OrcStruct value,
                  OutputCollector<OrcKey,OrcValue> output,
                  Reporter reporter) throws IOException {
    keyWrapper.key = value;
    valueWrapper.value = outStruct;
    int val = ((IntWritable) value.getFieldValue("i")).get();
    i.set(val * 2);
    j.set(val * val);
    output.collect(keyWrapper, valueWrapper);
  }

  public void configure(JobConf conf) { }

  public void close() { }
}
~~~

The reducer code accesses the underlying OrcStructs by using the
OrcKey.key and OrcValue.value fields.