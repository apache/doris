/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <p>
 * This package provides convenient access to ORC files using Hadoop's
 * MapReduce InputFormat and OutputFormat.
 * </p>
 *
 * <p>
 * For reading, set the InputFormat to OrcInputFormat and your map will
 * receive a stream of OrcStruct objects for each row. (Note that ORC files
 * may have any type as the root object instead of structs and then the
 * object type will be the appropriate one.)
 * </p>
 *
 * <p>The mapping of types is:</p>
 * <table summary="Mapping of ORC types to Writable types"
 *        border="1">
 *   <thead>
 *     <tr><th>ORC Type</th><th>Writable Type</th></tr>
 *   </thead>
 *   <tbody>
 *     <tr><td>array</td><td>OrcList</td></tr>
 *     <tr><td>binary</td><td>BytesWritable</td></tr>
 *     <tr><td>bigint</td><td>LongWritable</td></tr>
 *     <tr><td>boolean</td><td>BooleanWritable</td></tr>
 *     <tr><td>char</td><td>Text</td></tr>
 *     <tr><td>date</td><td>DateWritable</td></tr>
 *     <tr><td>decimal</td><td>HiveDecimalWritable</td></tr>
 *     <tr><td>double</td><td>DoubleWritable</td></tr>
 *     <tr><td>float</td><td>FloatWritable</td></tr>
 *     <tr><td>int</td><td>IntWritable</td></tr>
 *     <tr><td>map</td><td>OrcMap</td></tr>
 *     <tr><td>smallint</td><td>ShortWritable</td></tr>
 *     <tr><td>string</td><td>Text</td></tr>
 *     <tr><td>struct</td><td>OrcStruct</td></tr>
 *     <tr><td>timestamp</td><td>OrcTimestamp</td></tr>
 *     <tr><td>tinyint</td><td>ByteWritable</td></tr>
 *     <tr><td>uniontype</td><td>OrcUnion</td></tr>
 *     <tr><td>varchar</td><td>Text</td></tr>
 *   </tbody>
 * </table>
 *
 * <p>
 * For writing, set the OutputFormat to OrcOutputFormat and define the
 * property "orc.schema" in your configuration. The property defines the
 * type of the file and uses the Hive type strings, such as
 * "struct&lt;x:int,y:string,z:timestamp&gt;" for a row with an integer,
 * string, and timestamp. You can create an example object using:</p>
 *<pre>{@code
 *String typeStr = "struct<x:int,y:string,z:timestamp>";
 *OrcStruct row = (OrcStruct) OrcStruct.createValue(
 *    TypeDescription.fromString(typeStr));
 *}</pre>
 *
 * <p>
 * Please look at the OrcConf class for the configuration knobs that are
 * available.
 * </p>
 */
package org.apache.orc.mapred;
