/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.tools;

import com.google.gson.stream.JsonWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.CollectionColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * File dump tool with json formatted output.
 */
public class JsonFileDump {

  public static void printJsonMetaData(List<String> files,
      Configuration conf,
      List<Integer> rowIndexCols, boolean prettyPrint, boolean printTimeZone)
      throws IOException {
    if (files.isEmpty()) {
      return;
    }
    StringWriter stringWriter = new StringWriter();
    JsonWriter writer = new JsonWriter(stringWriter);
    if (prettyPrint) {
      writer.setIndent("  ");
    }
    boolean multiFile = files.size() > 1;
    if (multiFile) {
      writer.beginArray();
    } else {
      writer.beginObject();
    }
    for (String filename : files) {
      try {
        if (multiFile) {
          writer.beginObject();
        }
        writer.name("fileName").value(filename);
        Path path = new Path(filename);
        Reader reader = FileDump.getReader(path, conf, null);
        if (reader == null) {
          writer.name("status").value("FAILED");
          continue;
        }
        writer.name("fileVersion").value(reader.getFileVersion().getName());
        writer.name("writerVersion").value(reader.getWriterVersion().toString());
        writer.name("softwareVersion").value(reader.getSoftwareVersion());
        RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
        writer.name("numberOfRows").value(reader.getNumberOfRows());
        writer.name("compression").value(reader.getCompressionKind().toString());
        if (reader.getCompressionKind() != CompressionKind.NONE) {
          writer.name("compressionBufferSize").value(reader.getCompressionSize());
        }
        writer.name("schemaString").value(reader.getSchema().toString());
        writer.name("schema");
        writeSchema(writer, reader.getSchema());
        writer.name("calendar").value(reader.writerUsedProlepticGregorian()
            ? "proleptic Gregorian"
            : "Julian/Gregorian");
        writer.name("stripeStatistics").beginArray();
        List<StripeStatistics> stripeStatistics = reader.getStripeStatistics();
        for (int n = 0; n < stripeStatistics.size(); n++) {
          writer.beginObject();
          writer.name("stripeNumber").value(n + 1);
          StripeStatistics ss = stripeStatistics.get(n);
          writer.name("columnStatistics").beginArray();
          for (int i = 0; i < ss.getColumnStatistics().length; i++) {
            writer.beginObject();
            writer.name("columnId").value(i);
            writeColumnStatistics(writer, ss.getColumnStatistics()[i]);
            writer.endObject();
          }
          writer.endArray();
          writer.endObject();
        }
        writer.endArray();

        ColumnStatistics[] stats = reader.getStatistics();
        int colCount = stats.length;
        if (rowIndexCols == null) {
          rowIndexCols = new ArrayList<>(colCount);
          for (int i = 0; i < colCount; ++i) {
            rowIndexCols.add(i);
          }
        }
        writer.name("fileStatistics").beginArray();
        for (int i = 0; i < stats.length; ++i) {
          writer.beginObject();
          writer.name("columnId").value(i);
          writeColumnStatistics(writer, stats[i]);
          writer.endObject();
        }
        writer.endArray();

        writer.name("stripes").beginArray();
        int stripeIx = -1;
        for (StripeInformation stripe : reader.getStripes()) {
          ++stripeIx;
          long stripeStart = stripe.getOffset();
          OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
          writer.beginObject(); // start of stripe information
          writer.name("stripeNumber").value(stripeIx + 1);
          writer.name("stripeInformation");
          writeStripeInformation(writer, stripe);
          if (printTimeZone) {
            writer.name("writerTimezone").value(
                footer.hasWriterTimezone() ? footer.getWriterTimezone() : FileDump.UNKNOWN);
          }
          long sectionStart = stripeStart;

          writer.name("streams").beginArray();
          for (OrcProto.Stream section : footer.getStreamsList()) {
            writer.beginObject();
            String kind = section.hasKind() ? section.getKind().name() : FileDump.UNKNOWN;
            writer.name("columnId").value(section.getColumn());
            writer.name("section").value(kind);
            writer.name("startOffset").value(sectionStart);
            writer.name("length").value(section.getLength());
            sectionStart += section.getLength();
            writer.endObject();
          }
          writer.endArray();

          writer.name("encodings").beginArray();
          for (int i = 0; i < footer.getColumnsCount(); ++i) {
            writer.beginObject();
            OrcProto.ColumnEncoding encoding = footer.getColumns(i);
            writer.name("columnId").value(i);
            writer.name("kind").value(encoding.getKind().toString());
            if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
                encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
              writer.name("dictionarySize").value(encoding.getDictionarySize());
            }
            writer.endObject();
          }
          writer.endArray();
          if (!rowIndexCols.isEmpty()) {
            // include the columns that are specified, only if the columns are included, bloom filter
            // will be read
            boolean[] sargColumns = new boolean[colCount];
            for (int colIdx : rowIndexCols) {
              sargColumns[colIdx] = true;
            }
            OrcIndex indices = rows.readRowIndex(stripeIx, null, sargColumns);
            writer.name("indexes").beginArray();
            for (int col : rowIndexCols) {
              writer.beginObject();
              writer.name("columnId").value(col);
              writeRowGroupIndexes(writer, col, indices.getRowGroupIndex(),
                  reader.getSchema(), (ReaderImpl) reader);
              writeBloomFilterIndexes(writer, col, indices,
                  reader.getWriterVersion(),
                  reader.getSchema().findSubtype(col).getCategory(),
                  footer.getColumns(col));
              writer.endObject();
            }
            writer.endArray();
          }
          writer.endObject(); // end of stripe information
        }
        writer.endArray();

        FileSystem fs = path.getFileSystem(conf);
        long fileLen = fs.getContentSummary(path).getLength();
        long paddedBytes = FileDump.getTotalPaddingSize(reader);
        // empty ORC file is ~45 bytes. Assumption here is file length always >0
        double percentPadding = ((double) paddedBytes / (double) fileLen) * 100;
        writer.name("fileLength").value(fileLen);
        writer.name("paddingLength").value(paddedBytes);
        writer.name("paddingRatio").value(percentPadding);
        AcidStats acidStats = OrcAcidUtils.parseAcidStats(reader);
        if (acidStats != null) {
          writer.name("numInserts").value(acidStats.inserts);
          writer.name("numDeletes").value(acidStats.deletes);
          writer.name("numUpdates").value(acidStats.updates);
        }
        writer.name("status").value("OK");
        rows.close();

        writer.endObject();
      } catch (Throwable e) {
        writer.name("status").value("FAILED");
        throw e;
      }
    }
    if (multiFile) {
      writer.endArray();
    }
    System.out.println(stringWriter);
  }

  private static void writeSchema(JsonWriter writer, TypeDescription type)
      throws IOException {
    writer.beginObject();
    writer.name("columnId").value(type.getId());
    writer.name("columnType").value(type.getCategory().toString());
    List<String> attributes = type.getAttributeNames();
    if (attributes.size() > 0) {
      writer.name("attributes").beginObject();
      for (String name : attributes) {
        writer.name(name).value(type.getAttributeValue(name));
      }
      writer.endObject();
    }
    switch (type.getCategory()) {
      case DECIMAL:
        writer.name("precision").value(type.getPrecision());
        writer.name("scale").value(type.getScale());
        break;
      case VARCHAR:
      case CHAR:
        writer.name("maxLength").value(type.getMaxLength());
        break;
      default:
        break;
    }
    List<TypeDescription> children = type.getChildren();
    if (children != null) {
      writer.name("children");
      switch (type.getCategory()) {
        case STRUCT:
          writer.beginObject();
          List<String> fields = type.getFieldNames();
          for (int c = 0; c < fields.size(); ++c) {
            writer.name(fields.get(c));
            writeSchema(writer, children.get(c));
          }
          writer.endObject();
          break;
        case LIST:
          writer.beginArray();
          writeSchema(writer, children.get(0));
          writer.endArray();
          break;
        case MAP:
          writer.beginArray();
          writeSchema(writer, children.get(0));
          writeSchema(writer, children.get(1));
          writer.endArray();
          break;
        case UNION:
          writer.beginArray();
          for (TypeDescription child : children) {
            writeSchema(writer, child);
          }
          writer.endArray();
          break;
        default:
          break;
      }
    }
    writer.endObject();
  }

  private static void writeStripeInformation(JsonWriter writer, StripeInformation stripe)
      throws IOException {
    writer.beginObject();
    writer.name("offset").value(stripe.getOffset());
    writer.name("indexLength").value(stripe.getIndexLength());
    writer.name("dataLength").value(stripe.getDataLength());
    writer.name("footerLength").value(stripe.getFooterLength());
    writer.name("rowCount").value(stripe.getNumberOfRows());
    writer.endObject();
  }

  private static void writeColumnStatistics(JsonWriter writer, ColumnStatistics cs)
      throws IOException {
    if (cs != null) {
      writer.name("count").value(cs.getNumberOfValues());
      writer.name("hasNull").value(cs.hasNull());
      if (cs.getBytesOnDisk() != 0) {
        writer.name("bytesOnDisk").value(cs.getBytesOnDisk());
      }
      if (cs instanceof BinaryColumnStatistics) {
        writer.name("totalLength").value(((BinaryColumnStatistics) cs).getSum());
        writer.name("type").value(OrcProto.Type.Kind.BINARY.toString());
      } else if (cs instanceof BooleanColumnStatistics) {
        writer.name("trueCount").value(((BooleanColumnStatistics) cs).getTrueCount());
        writer.name("falseCount").value(((BooleanColumnStatistics) cs).getFalseCount());
        writer.name("type").value(OrcProto.Type.Kind.BOOLEAN.toString());
      } else if (cs instanceof IntegerColumnStatistics) {
        writer.name("min").value(((IntegerColumnStatistics) cs).getMinimum());
        writer.name("max").value(((IntegerColumnStatistics) cs).getMaximum());
        if (((IntegerColumnStatistics) cs).isSumDefined()) {
          writer.name("sum").value(((IntegerColumnStatistics) cs).getSum());
        }
        writer.name("type").value(OrcProto.Type.Kind.LONG.toString());
      } else if (cs instanceof DoubleColumnStatistics) {
        writer.name("min").value(((DoubleColumnStatistics) cs).getMinimum());
        writer.name("max").value(((DoubleColumnStatistics) cs).getMaximum());
        writer.name("sum").value(((DoubleColumnStatistics) cs).getSum());
        writer.name("type").value(OrcProto.Type.Kind.DOUBLE.toString());
      } else if (cs instanceof StringColumnStatistics) {
        String lower = ((StringColumnStatistics) cs).getLowerBound();
        if (((StringColumnStatistics) cs).getMinimum() != null) {
          writer.name("min").value(lower);
        } else if (lower != null) {
          writer.name("lowerBound").value(lower);
        }
        String upper = ((StringColumnStatistics) cs).getUpperBound();
        if (((StringColumnStatistics) cs).getMaximum() != null) {
          writer.name("max").value(upper);
        } else if (upper != null) {
          writer.name("upperBound").value(upper);
        }
        writer.name("totalLength").value(((StringColumnStatistics) cs).getSum());
        writer.name("type").value(OrcProto.Type.Kind.STRING.toString());
      } else if (cs instanceof DateColumnStatistics) {
        if (((DateColumnStatistics) cs).getMaximumLocalDate() != null) {
          writer.name("min").value(((DateColumnStatistics) cs).getMinimumLocalDate().toString());
          writer.name("max").value(((DateColumnStatistics) cs).getMaximumLocalDate().toString());
        }
        writer.name("type").value(OrcProto.Type.Kind.DATE.toString());
      } else if (cs instanceof TimestampColumnStatistics) {
        if (((TimestampColumnStatistics) cs).getMaximum() != null) {
          writer.name("min").value(((TimestampColumnStatistics) cs).getMinimum().toString());
          writer.name("max").value(((TimestampColumnStatistics) cs).getMaximum().toString());
        }
        writer.name("type").value(OrcProto.Type.Kind.TIMESTAMP.toString());
      } else if (cs instanceof DecimalColumnStatistics) {
        if (((DecimalColumnStatistics) cs).getMaximum() != null) {
          writer.name("min").value(((DecimalColumnStatistics) cs).getMinimum().toString());
          writer.name("max").value(((DecimalColumnStatistics) cs).getMaximum().toString());
          writer.name("sum").value(((DecimalColumnStatistics) cs).getSum().toString());
        }
        writer.name("type").value(OrcProto.Type.Kind.DECIMAL.toString());
      } else if (cs instanceof CollectionColumnStatistics) {
        writer.name("minChildren").value(((CollectionColumnStatistics) cs).getMinimumChildren());
        writer.name("maxChildren").value(((CollectionColumnStatistics) cs).getMaximumChildren());
        writer.name("totalChildren").value(((CollectionColumnStatistics) cs).getTotalChildren());
      }
    }
  }

  private static void writeBloomFilterIndexes(JsonWriter writer, int col,
                                              OrcIndex index,
                                              OrcFile.WriterVersion version,
                                              TypeDescription.Category type,
                                              OrcProto.ColumnEncoding encoding
  ) throws IOException {

    BloomFilter stripeLevelBF = null;
    OrcProto.BloomFilterIndex[] bloomFilterIndex = index.getBloomFilterIndex();
    if (bloomFilterIndex != null && bloomFilterIndex[col] != null) {
      int entryIx = 0;
      writer.name("bloomFilterIndexes").beginArray();
      for (OrcProto.BloomFilter bf : bloomFilterIndex[col].getBloomFilterList()) {
        writer.beginObject();
        writer.name("entryId").value(entryIx++);
        BloomFilter toMerge = BloomFilterIO.deserialize(
            index.getBloomFilterKinds()[col], encoding, version, type, bf);
        writeBloomFilterStats(writer, toMerge);
        if (stripeLevelBF == null) {
          stripeLevelBF = toMerge;
        } else {
          stripeLevelBF.merge(toMerge);
        }
        writer.endObject();
      }
      writer.endArray();
    }
    if (stripeLevelBF != null) {
      writer.name("stripeLevelBloomFilter");
      writer.beginObject();
      writeBloomFilterStats(writer, stripeLevelBF);
      writer.endObject();
    }
  }

  private static void writeBloomFilterStats(JsonWriter writer, BloomFilter bf)
      throws IOException {
    int bitCount = bf.getBitSize();
    int popCount = 0;
    for (long l : bf.getBitSet()) {
      popCount += Long.bitCount(l);
    }
    int k = bf.getNumHashFunctions();
    float loadFactor = (float) popCount / (float) bitCount;
    float expectedFpp = (float) Math.pow(loadFactor, k);
    writer.name("numHashFunctions").value(k);
    writer.name("bitCount").value(bitCount);
    writer.name("popCount").value(popCount);
    writer.name("loadFactor").value(loadFactor);
    writer.name("expectedFpp").value(expectedFpp);
  }

  private static void writeRowGroupIndexes(JsonWriter writer, int col,
                                           OrcProto.RowIndex[] rowGroupIndex,
                                           TypeDescription schema,
                                           ReaderImpl reader) throws IOException {
    OrcProto.RowIndex index;
    if (rowGroupIndex == null || (col >= rowGroupIndex.length) ||
        ((index = rowGroupIndex[col]) == null)) {
      return;
    }

    writer.name("rowGroupIndexes").beginArray();
    for (int entryIx = 0; entryIx < index.getEntryCount(); ++entryIx) {
      writer.beginObject();
      writer.name("entryId").value(entryIx);
      OrcProto.RowIndexEntry entry = index.getEntry(entryIx);
      if (entry == null || !entry.hasStatistics()) {
        continue;
      }
      OrcProto.ColumnStatistics colStats = entry.getStatistics();
      writeColumnStatistics(writer, ColumnStatisticsImpl.deserialize(
          schema.findSubtype(col), colStats, reader.writerUsedProlepticGregorian(),
          reader.getConvertToProlepticGregorian()));
      writer.name("positions").beginArray();
      for (int posIx = 0; posIx < entry.getPositionsCount(); ++posIx) {
        writer.value(entry.getPositions(posIx));
      }
      writer.endArray();
      writer.endObject();
    }
    writer.endArray();
  }

}
