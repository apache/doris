/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.mapred;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.Writer;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test the behavior of ORC's schema evolution
 */
public class TestOrcFileEvolution {

  // These utility methods are just to make writing tests easier. The values
  // created here will not feed directly to the ORC writers, but are converted
  // within checkEvolution().
  private List<Object> struct(Object... fields) {
    return list(fields);
  }

  private List<Object> list(Object... elements) {
    return Arrays.asList(elements);
  }

  private Map<Object, Object> map(Object... kvs) {
    if (kvs.length != 2) {
      throw new IllegalArgumentException(
          "Map must be provided an even number of arguments");
    }

    Map<Object, Object> result = new HashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      result.put(kvs[i], kvs[i + 1]);
    }
    return result;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testAddFieldToEnd(boolean addSarg) {
    checkEvolution("struct<a:int,b:string>", "struct<a:int,b:string,c:double>",
        struct(11, "foo"),
        addSarg ? struct(0, "", 0.0) : struct(11, "foo", null),
        addSarg);
  }

  @Test
  public void testAddFieldToEndWithSarg() {
    SearchArgument sArg = SearchArgumentFactory
      .newBuilder()
      .lessThan("c", PredicateLeaf.Type.LONG, 10L)
      .build();
    String[] sCols = new String[]{null, null, "c"};

    checkEvolution("struct<a:int,b:string>", "struct<a:int,b:string,c:int>",
                   struct(1, "foo"),
                   struct(1, "foo", null),
                   (boolean) OrcConf.TOLERATE_MISSING_SCHEMA.getDefaultValue(),
                   sArg, sCols, false);
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testAddFieldBeforeEnd(boolean addSarg) {
    checkEvolution("struct<a:int,b:string>", "struct<a:int,c:double,b:string>",
        struct(1, "foo"),
        struct(1, null, "foo"),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testRemoveLastField(boolean addSarg) {
    checkEvolution("struct<a:int,b:string,c:double>", "struct<a:int,b:string>",
        struct(1, "foo", 3.14),
        struct(1, "foo"),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testRemoveFieldBeforeEnd(boolean addSarg) {
    checkEvolution("struct<a:int,b:string,c:double>", "struct<a:int,c:double>",
        struct(1, "foo", 3.14),
        struct(1, 3.14),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  public void testRemoveAndAddField(boolean addSarg) {
    checkEvolution("struct<a:int,b:string>", "struct<a:int,c:double>",
        struct(1, "foo"), struct(1, null),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testReorderFields(boolean addSarg) {
    checkEvolution("struct<a:int,b:string>", "struct<b:string,a:int>",
        struct(1, "foo"), struct("foo", 1),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAddFieldEndOfStruct(boolean addSarg) {
    checkEvolution("struct<a:struct<b:int>,c:string>",
        "struct<a:struct<b:int,d:double>,c:string>",
        struct(struct(2), "foo"), struct(struct(2, null), "foo"),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAddFieldBeforeEndOfStruct(boolean addSarg) {
    checkEvolution("struct<a:struct<b:int>,c:string>",
        "struct<a:struct<d:double,b:int>,c:string>",
        struct(struct(2), "foo"), struct(struct(null, 2), "foo"),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAddSimilarField(boolean addSarg) {
    checkEvolution("struct<a:struct<b:int>>",
        "struct<a:struct<b:int>,c:struct<b:int>>", struct(struct(2)),
        struct(struct(2), null),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConvergentEvolution(boolean addSarg) {
    checkEvolution("struct<a:struct<a:int,b:string>,c:struct<a:int>>",
        "struct<a:struct<a:int,b:string>,c:struct<a:int,b:string>>",
        struct(struct(2, "foo"), struct(3)),
        struct(struct(2, "foo"), struct(3, null)),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMapKeyEvolution(boolean addSarg) {
    checkEvolution("struct<a:map<struct<a:int>,int>>",
        "struct<a:map<struct<a:int,b:string>,int>>",
        struct(map(struct(1), 2)),
        struct(map(struct(1, null), 2)),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMapValueEvolution(boolean addSarg) {
    checkEvolution("struct<a:map<int,struct<a:int>>>",
        "struct<a:map<int,struct<a:int,b:string>>>",
        struct(map(2, struct(1))),
        struct(map(2, struct(1, null))),
                   addSarg);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListEvolution(boolean addSarg) {
    checkEvolution("struct<a:array<struct<b:int>>>",
        "struct<a:array<struct<b:int,c:string>>>",
        struct(list(struct(1), struct(2))),
        struct(list(struct(1, null), struct(2, null))),
                   addSarg);
  }

  @Test
  public void testMissingColumnFromReaderSchema() {
    // If column is part of the SArg but is missing from the reader schema
    // will be ignored (consistent with 1.6 release behaviour)
    checkEvolution("struct<b:int,c:string>",
       "struct<b:int,c:string>",
        struct(1, "foo"),
        struct(1, "foo", null),
        true, true, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreHive4243CheckEqual(boolean addSarg) {
    // Expect success on equal schemas
    checkEvolutionPosn("struct<_col0:int,_col1:string>",
                   "struct<_col0:int,_col1:string>",
                   struct(1, "foo"),
                   struct(1, "foo", null), false, addSarg, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreHive4243Check(boolean addSarg) {
    // Expect exception on strict compatibility check
    Exception e = assertThrows(RuntimeException.class, () -> {
      checkEvolutionPosn("struct<_col0:int,_col1:string>",
        "struct<_col0:int,_col1:string,_col2:double>",
        struct(1, "foo"),
        struct(1, "foo", null), false, addSarg, false);
    });
    assertTrue(e.getMessage().contains("HIVE-4243"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreHive4243AddColumn(boolean addSarg) {
    checkEvolutionPosn("struct<_col0:int,_col1:string>",
                   "struct<_col0:int,_col1:string,_col2:double>",
                   struct(1, "foo"),
                   struct(1, "foo", null), true, addSarg, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreHive4243AddColumnMiddle(boolean addSarg) {
    // Expect exception on type mismatch
    assertThrows(SchemaEvolution.IllegalEvolutionException.class, () -> {
      checkEvolutionPosn("struct<_col0:int,_col1:double>",
        "struct<_col0:int,_col1:date,_col2:double>",
        struct(1, 1.0),
        null, true, addSarg, false);
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreHive4243AddColumnWithFix(boolean addSarg) {
    checkEvolution("struct<_col0:int,_col1:string>",
                   "struct<a:int,b:string,c:double>",
                   struct(1, "foo"),
                   struct(1, "foo", null), true, addSarg, false);
  }

  @Test
  public void testPreHive4243AddColumnMiddleWithFix() {
    // Expect exception on type mismatch
    assertThrows(SchemaEvolution.IllegalEvolutionException.class, () -> {
      checkEvolution("struct<_col0:int,_col1:double>",
              "struct<a:int,b:date,c:double>",
              struct(1, 1.0),
              null, true);
    });
  }

  /**
   * Test positional schema evolution.
   * With the sarg, it eliminates the row and we don't get the row.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPositional(boolean addSarg) {
    checkEvolution("struct<x:int,y:int,z:int>", "struct<a:int,b:int,c:int,d:int>",
        struct(11, 2, 3),
        // if the sarg works, we get the default value
        addSarg ? struct(0, 0, 0, 0) : struct(11, 2, 3, null),
        false, addSarg, true);
  }

  /**
   * Make the sarg try to use a column past the end of the file schema, since
   * it will get null, the predicate doesn't hit.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPositional2(boolean addSarg) {
    checkEvolution("struct<x:int,y:int,z:int>", "struct<b:int,c:int,d:int,a:int>",
        struct(11, 2, 3),
        struct(11, 2, 3, null),
        false, addSarg, true);
  }

  private void checkEvolutionPosn(String writerType, String readerType,
                                  Object inputRow, Object expectedOutput,
                                  boolean tolerateSchema, boolean addSarg,
                                  boolean positional) {
    SearchArgument sArg = null;
    String[] sCols = null;
    if (addSarg) {
      sArg = SearchArgumentFactory
        .newBuilder()
        .lessThan("_col0", PredicateLeaf.Type.LONG, 10L)
        .build();
      sCols = new String[]{null, "_col0", null};
    }

    checkEvolution(writerType, readerType,
                   inputRow, expectedOutput,
                   tolerateSchema,
                   sArg, sCols, positional);
  }

  private void checkEvolution(String writerType, String readerType,
                              Object inputRow, Object expectedOutput,
                              boolean tolerateSchema, boolean addSarg,
                              boolean positional) {
    SearchArgument sArg = null;
    String[] sCols = null;
    if (addSarg) {
      sArg = SearchArgumentFactory
        .newBuilder()
        .lessThan("a", PredicateLeaf.Type.LONG, 10L)
        .build();
      sCols = new String[]{null, "a", null};
    }

    checkEvolution(writerType, readerType,
                   inputRow, expectedOutput,
                   tolerateSchema,
                   sArg, sCols, positional);
  }

  private void checkEvolution(String writerType, String readerType,
                              Object inputRow, Object expectedOutput,
                              boolean addSarg) {

    checkEvolution(writerType, readerType,
      inputRow, expectedOutput,
      (boolean) OrcConf.TOLERATE_MISSING_SCHEMA.getDefaultValue(),
        addSarg, false);
  }

  private void checkEvolution(String writerType, String readerType,
                              Object inputRow, Object expectedOutput,
                              boolean tolerateSchema, SearchArgument sArg,
                              String[] sCols, boolean positional) {
    TypeDescription readTypeDescr = TypeDescription.fromString(readerType);
    TypeDescription writerTypeDescr = TypeDescription.fromString(writerType);

    OrcStruct inputStruct = assembleStruct(writerTypeDescr, inputRow);
    OrcStruct expectedStruct = assembleStruct(readTypeDescr, expectedOutput);
    try {
      Writer writer = OrcFile.createWriter(testFilePath,
          OrcFile.writerOptions(conf).setSchema(writerTypeDescr)
              .stripeSize(100000).bufferSize(10000)
              .version(OrcFile.Version.CURRENT));

      OrcMapredRecordWriter<OrcStruct> recordWriter =
          new OrcMapredRecordWriter<OrcStruct>(writer);
      recordWriter.write(NullWritable.get(), inputStruct);
      recordWriter.close(mock(Reporter.class));

      Reader reader = OrcFile.createReader(testFilePath,
                                           OrcFile.readerOptions(conf).filesystem(fs));

      Reader.Options options = reader.options().schema(readTypeDescr);
      if (sArg != null && sCols != null) {
        options.searchArgument(sArg, sCols).allowSARGToFilter(false);
      }

      OrcMapredRecordReader<OrcStruct> recordReader =
          new OrcMapredRecordReader<>(reader,
              options.tolerateMissingSchema(tolerateSchema)
                  .forcePositionalEvolution(positional));
      OrcStruct result = recordReader.createValue();
      recordReader.next(recordReader.createKey(), result);
      assertEquals(expectedStruct, result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private OrcStruct assembleStruct(TypeDescription type, Object row) {
    Preconditions.checkArgument(
        type.getCategory() == Category.STRUCT, "Top level type must be STRUCT");

    return (OrcStruct) assembleRecord(type, row);
  }

  private WritableComparable assembleRecord(TypeDescription type, Object row) {
    if (row == null) {
      return null;
    }
    switch (type.getCategory()) {
    case STRUCT:
      OrcStruct structResult = new OrcStruct(type);
      for (int i = 0; i < structResult.getNumFields(); i++) {
        List<TypeDescription> childTypes = type.getChildren();
        structResult.setFieldValue(i,
            assembleRecord(childTypes.get(i), ((List<Object>) row).get(i)));
      }
      return structResult;
    case LIST:
      OrcList<WritableComparable> listResult = new OrcList<>(type);
      TypeDescription elemType = type.getChildren().get(0);
      List<Object> elems = (List<Object>) row;
      for (int i = 0; i < elems.size(); i++) {
        listResult.add(assembleRecord(elemType, elems.get(i)));
      }
      return listResult;
    case MAP:
      OrcMap<WritableComparable, WritableComparable> mapResult =
          new OrcMap<>(type);
      TypeDescription keyType = type.getChildren().get(0);
      TypeDescription valueType = type.getChildren().get(1);
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) row)
          .entrySet()) {
        mapResult.put(assembleRecord(keyType, entry.getKey()),
            assembleRecord(valueType, entry.getValue()));
      }
      return mapResult;
    case INT:
      return new IntWritable((Integer) row);
    case DOUBLE:
      return new DoubleWritable((Double) row);
    case STRING:
      return new Text((String) row);
    default:
      throw new UnsupportedOperationException(String
          .format("Not expecting to have a field of type %s in unit tests",
              type.getCategory()));
    }
  }
}
