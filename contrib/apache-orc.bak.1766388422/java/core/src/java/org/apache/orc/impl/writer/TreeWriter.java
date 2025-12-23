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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * The writers for the specific writers of each type. This provides
 * the generic API that they must all implement.
 */
public interface TreeWriter {

  /**
   * Estimate the memory currently used to buffer the stripe.
   * @return the number of bytes
   */
  long estimateMemory();

  /**
   * Estimate the memory used if the file was read into Hive's Writable
   * types. This is used as an estimate for the query optimizer.
   * @return the number of bytes
   */
  long getRawDataSize();

  /**
   * Set up for the next stripe.
   * @param stripeId the next stripe id
   */
  void prepareStripe(int stripeId);

  /**
   * Write a VectorizedRowBatch to the file. This is called by the WriterImplV2
   * at the top level.
   * @param batch the list of all of the columns
   * @param offset the first row from the batch to write
   * @param length the number of rows to write
   */
  void writeRootBatch(VectorizedRowBatch batch, int offset,
                      int length) throws IOException;

  /**
   * Write a ColumnVector to the file. This is called recursively by
   * writeRootBatch.
   * @param vector the data to write
   * @param offset the first value offset to write.
   * @param length the number of values to write
   */
  void writeBatch(ColumnVector vector, int offset,
                  int length) throws IOException;

  /**
   * Create a row index entry at the current point in the stripe.
   */
  void createRowIndexEntry() throws IOException;

  /**
   * Flush the TreeWriter stream
   * @throws IOException
   */
  void flushStreams() throws IOException;

  /**
   * Write the stripe out to the file.
   * @param requiredIndexEntries the number of index entries that are
   *                             required. this is to check to make sure the
   *                             row index is well formed.
   */
  void writeStripe(int requiredIndexEntries) throws IOException;

  /**
   * During a stripe append, we need to handle the stripe statistics.
   * @param stripeStatistics the statistics for the new stripe across the
   *                         encryption variants
   */
  void addStripeStatistics(StripeStatistics[] stripeStatistics
                           ) throws IOException;

  /**
   * Write the FileStatistics for each column in each encryption variant.
   */
  void writeFileStatistics() throws IOException;

  /**
   * Get the current file statistics for each column. If a column is encrypted,
   * the encrypted variant statistics are used.
   * @param output an array that is filled in with the results
   */
  void getCurrentStatistics(ColumnStatistics[] output);

  class Factory {
    /**
     * Create a new tree writer for the given types and insert encryption if
     * required.
     * @param schema the type to build a writer for
     * @param encryption the encryption status
     * @param streamFactory the writer context
     * @return a new tree writer
     */
    public static TreeWriter create(TypeDescription schema,
                                    WriterEncryptionVariant encryption,
                                    WriterContext streamFactory) throws IOException {
      if (encryption == null) {
        // If we are the root of an encryption variant, create a special writer.
        encryption = streamFactory.getEncryption(schema.getId());
        if (encryption != null) {
          return new EncryptionTreeWriter(schema, encryption, streamFactory);
        }
      }
      return createSubtree(schema, encryption, streamFactory);
    }

    /**
     * Create a subtree without inserting encryption nodes
     * @param schema the schema to create
     * @param encryption the encryption variant
     * @param streamFactory the writer context
     * @return a new tree writer
     */
    static TreeWriter createSubtree(TypeDescription schema,
                                    WriterEncryptionVariant encryption,
                                    WriterContext streamFactory) throws IOException {
      OrcFile.Version version = streamFactory.getVersion();
      switch (schema.getCategory()) {
        case BOOLEAN:
          return new BooleanTreeWriter(schema, encryption, streamFactory);
        case BYTE:
          return new ByteTreeWriter(schema, encryption, streamFactory);
        case SHORT:
        case INT:
        case LONG:
          return new IntegerTreeWriter(schema, encryption, streamFactory);
        case FLOAT:
          return new FloatTreeWriter(schema, encryption, streamFactory);
        case DOUBLE:
          return new DoubleTreeWriter(schema, encryption, streamFactory);
        case STRING:
          return new StringTreeWriter(schema, encryption, streamFactory);
        case CHAR:
          return new CharTreeWriter(schema, encryption, streamFactory);
        case VARCHAR:
          return new VarcharTreeWriter(schema, encryption, streamFactory);
        case BINARY:
          return new BinaryTreeWriter(schema, encryption, streamFactory);
        case TIMESTAMP:
          return new TimestampTreeWriter(schema, encryption, streamFactory, false);
        case TIMESTAMP_INSTANT:
          return new TimestampTreeWriter(schema, encryption, streamFactory, true);
        case DATE:
          return new DateTreeWriter(schema, encryption, streamFactory);
        case DECIMAL:
          if (version == OrcFile.Version.UNSTABLE_PRE_2_0 &&
                  schema.getPrecision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
            return new Decimal64TreeWriter(schema, encryption, streamFactory);
          }
          return new DecimalTreeWriter(schema, encryption, streamFactory);
        case STRUCT:
          return new StructTreeWriter(schema, encryption, streamFactory);
        case MAP:
          return new MapTreeWriter(schema, encryption, streamFactory);
        case LIST:
          return new ListTreeWriter(schema, encryption, streamFactory);
        case UNION:
          return new UnionTreeWriter(schema, encryption, streamFactory);
        default:
          throw new IllegalArgumentException("Bad category: " +
                                               schema.getCategory());
      }
    }
  }
}
