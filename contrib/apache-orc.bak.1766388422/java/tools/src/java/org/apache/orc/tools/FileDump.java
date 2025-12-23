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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcAcidUtils;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {
  public static final String UNKNOWN = "UNKNOWN";
  public static final String SEPARATOR = StringUtils.repeat("_", 120) + "\n";
  public static final String RECOVER_READ_SIZE = "orc.recover.read.size"; // only for testing
  public static final int DEFAULT_BLOCK_SIZE = 256 * 1024 * 1024;
  public static final String DEFAULT_BACKUP_PATH = System.getProperty("java.io.tmpdir");
  public static final PathFilter HIDDEN_AND_SIDE_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(
          OrcAcidUtils.DELTA_SIDE_FILE_SUFFIX);
    }
  };

  // not used
  private FileDump() {
  }

  public static void main(Configuration conf, String[] args) throws Exception {
    List<Integer> rowIndexCols = new ArrayList<Integer>(0);
    Options opts = createOptions();
    CommandLine cli = new DefaultParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("orcfiledump", opts);
      return;
    }

    boolean dumpData = cli.hasOption('d');
    boolean recover = cli.hasOption("recover");
    boolean skipDump = cli.hasOption("skip-dump");
    String backupPath = DEFAULT_BACKUP_PATH;
    if (cli.hasOption("backup-path")) {
      backupPath = cli.getOptionValue("backup-path");
    }

    if (cli.hasOption("r")) {
      String val = cli.getOptionValue("r");
      if (val != null && val.trim().equals("*")) {
        rowIndexCols = null; // All the columns
      } else {
        String[] colStrs = cli.getOptionValue("r").split(",");
        rowIndexCols = new ArrayList<Integer>(colStrs.length);
        for (String colStr : colStrs) {
          rowIndexCols.add(Integer.parseInt(colStr));
        }
      }
    }

    boolean printTimeZone = cli.hasOption('t');
    boolean jsonFormat = cli.hasOption('j');
    String[] files = cli.getArgs();
    if (files.length == 0) {
      System.err.println("Error : ORC files are not specified");
      return;
    }

    // if the specified path is directory, iterate through all files and print the file dump
    List<String> filesInPath = new ArrayList<>();
    for (String filename : files) {
      Path path = new Path(filename);
      filesInPath.addAll(getAllFilesInPath(path, conf));
    }

    if (dumpData) {
      PrintData.main(conf, filesInPath.toArray(new String[filesInPath.size()]));
    } else if (recover && skipDump) {
      recoverFiles(filesInPath, conf, backupPath);
    } else {
      if (jsonFormat) {
        boolean prettyPrint = cli.hasOption('p');
        JsonFileDump.printJsonMetaData(filesInPath, conf, rowIndexCols, prettyPrint, printTimeZone);
      } else {
        printMetaData(filesInPath, conf, rowIndexCols, printTimeZone, recover, backupPath);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    main(conf, args);
  }

  /**
   * This method returns an ORC reader object if the specified file is readable. If the specified
   * file has side file (_flush_length) file, then max footer offset will be read from the side
   * file and orc reader will be created from that offset. Since both data file and side file
   * use hflush() for flushing the data, there could be some inconsistencies and both files could be
   * out-of-sync. Following are the cases under which null will be returned
   *
   * 1) If the file specified by path or its side file is still open for writes
   * 2) If *_flush_length file does not return any footer offset
   * 3) If *_flush_length returns a valid footer offset but the data file is not readable at that
   *    position (incomplete data file)
   * 4) If *_flush_length file length is not a multiple of 8, then reader will be created from
   *    previous valid footer. If there is no such footer (file length > 0 and < 8), then null will
   *    be returned
   *
   * Also, if this method detects any file corruption (mismatch between data file and side file)
   * then it will add the corresponding file to the specified input list for corrupted files.
   *
   * In all other cases, where the file is readable this method will return a reader object.
   *
   * @param path - file to get reader for
   * @param conf - configuration object
   * @param corruptFiles - fills this list with all possible corrupted files
   * @return - reader for the specified file or null
   * @throws IOException
   */
  static Reader getReader(final Path path, final Configuration conf,
      final List<String> corruptFiles) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    long dataFileLen = fs.getFileStatus(path).getLen();
    System.err.println("Processing data file " + path + " [length: " + dataFileLen + "]");
    Path sideFile = OrcAcidUtils.getSideFile(path);
    final boolean sideFileExists = fs.exists(sideFile);
    boolean openDataFile = false;
    boolean openSideFile = false;
    if (fs instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      openDataFile = !dfs.isFileClosed(path);
      openSideFile = sideFileExists && !dfs.isFileClosed(sideFile);
    }

    if (openDataFile || openSideFile) {
      if (openDataFile && openSideFile) {
        System.err.println("Unable to perform file dump as " + path + " and " + sideFile +
            " are still open for writes.");
      } else if (openSideFile) {
        System.err.println("Unable to perform file dump as " + sideFile +
            " is still open for writes.");
      } else {
        System.err.println("Unable to perform file dump as " + path +
            " is still open for writes.");
      }

      return null;
    }

    Reader reader = null;
    if (sideFileExists) {
      final long maxLen = OrcAcidUtils.getLastFlushLength(fs, path);
      final long sideFileLen = fs.getFileStatus(sideFile).getLen();
      System.err.println("Found flush length file " + sideFile
          + " [length: " + sideFileLen + ", maxFooterOffset: " + maxLen + "]");
      // no offsets read from side file
      if (maxLen == -1) {

        // if data file is larger than last flush length, then additional data could be recovered
        if (dataFileLen > maxLen) {
          System.err.println("Data file has more data than max footer offset:" + maxLen +
              ". Adding data file to recovery list.");
          if (corruptFiles != null) {
            corruptFiles.add(path.toUri().toString());
          }
        }
        return null;
      }

      try {
        reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).maxLength(maxLen));

        // if data file is larger than last flush length, then additional data could be recovered
        if (dataFileLen > maxLen) {
          System.err.println("Data file has more data than max footer offset:" + maxLen +
              ". Adding data file to recovery list.");
          if (corruptFiles != null) {
            corruptFiles.add(path.toUri().toString());
          }
        }
      } catch (Exception e) {
        if (corruptFiles != null) {
          corruptFiles.add(path.toUri().toString());
        }
        System.err.println("Unable to read data from max footer offset." +
            " Adding data file to recovery list.");
        return null;
      }
    } else {
      reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    }

    return reader;
  }

  public static Collection<String> getAllFilesInPath(final Path path,
      final Configuration conf) throws IOException {
    List<String> filesInPath = new ArrayList<>();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus.isDirectory()) {
      FileStatus[] fileStatuses = fs.listStatus(path, HIDDEN_AND_SIDE_FILE_FILTER);
      for (FileStatus fileInPath : fileStatuses) {
        if (fileInPath.isDirectory()) {
          filesInPath.addAll(getAllFilesInPath(fileInPath.getPath(), conf));
        } else {
          filesInPath.add(fileInPath.getPath().toString());
        }
      }
    } else {
      filesInPath.add(path.toString());
    }

    return filesInPath;
  }

  private static void printMetaData(List<String> files, Configuration conf,
      List<Integer> rowIndexCols, boolean printTimeZone, final boolean recover,
      final String backupPath)
      throws IOException {
    List<String> corruptFiles = new ArrayList<>();
    for (String filename : files) {
      printMetaDataImpl(filename, conf, rowIndexCols, printTimeZone, corruptFiles);
      System.out.println(SEPARATOR);
    }

    if (!corruptFiles.isEmpty()) {
      if (recover) {
        recoverFiles(corruptFiles, conf, backupPath);
      } else {
        System.err.println(corruptFiles.size() + " file(s) are corrupted." +
            " Run the following command to recover corrupted files.\n");
        StringBuilder buffer = new StringBuilder();
        buffer.append("hive --orcfiledump --recover --skip-dump");
        for(String file: corruptFiles) {
          buffer.append(' ');
          buffer.append(file);
        }
        System.err.println(buffer);
        System.out.println(SEPARATOR);
      }
    }
  }

  static void printTypeAnnotations(TypeDescription type, String prefix) {
    List<String> attributes = type.getAttributeNames();
    if (attributes.size() > 0) {
      System.out.println("Attributes on " + prefix);
      for(String attr: attributes) {
        System.out.println("  " + attr + ": " + type.getAttributeValue(attr));
      }
    }
    List<TypeDescription> children = type.getChildren();
    if (children != null) {
      switch (type.getCategory()) {
        case STRUCT:
          List<String> fields = type.getFieldNames();
          for(int c = 0; c < children.size(); ++c) {
            printTypeAnnotations(children.get(c), prefix + "." + fields.get(c));
          }
          break;
        case MAP:
          printTypeAnnotations(children.get(0), prefix + "._key");
          printTypeAnnotations(children.get(1), prefix + "._value");
          break;
        case LIST:
          printTypeAnnotations(children.get(0), prefix + "._elem");
          break;
        case UNION:
          for(int c = 0; c < children.size(); ++c) {
            printTypeAnnotations(children.get(c), prefix + "._" + c);
          }
          break;
      }
    }
  }

  private static void printMetaDataImpl(final String filename,
      final Configuration conf, List<Integer> rowIndexCols, final boolean printTimeZone,
      final List<String> corruptFiles) throws IOException {
    Path file = new Path(filename);
    Reader reader = getReader(file, conf, corruptFiles);
    // if we can create reader then footer is not corrupt and file will readable
    if (reader == null) {
      return;
    }
    TypeDescription schema = reader.getSchema();
    System.out.println("Structure for " + filename);
    System.out.println("File Version: " + reader.getFileVersion().getName() +
        " with " + reader.getWriterVersion() + " by " +
        reader.getSoftwareVersion());
    RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
    System.out.println("Rows: " + reader.getNumberOfRows());
    System.out.println("Compression: " + reader.getCompressionKind());
    if (reader.getCompressionKind() != CompressionKind.NONE) {
      System.out.println("Compression size: " + reader.getCompressionSize());
    }
    System.out.println("Calendar: " + (reader.writerUsedProlepticGregorian()
                           ? "Proleptic Gregorian"
                           : "Julian/Gregorian"));
    System.out.println("Type: " + reader.getSchema().toString());
    printTypeAnnotations(reader.getSchema(), "root");
    System.out.println("\nStripe Statistics:");
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    for (int n = 0; n < stripeStats.size(); n++) {
      System.out.println("  Stripe " + (n + 1) + ":");
      StripeStatistics ss = stripeStats.get(n);
      for (int i = 0; i < ss.getColumnStatistics().length; ++i) {
        System.out.println("    Column " + i + ": " +
            ss.getColumnStatistics()[i].toString());
      }
    }
    ColumnStatistics[] stats = reader.getStatistics();
    int colCount = stats.length;
    if (rowIndexCols == null) {
      rowIndexCols = new ArrayList<>(colCount);
      for (int i = 0; i < colCount; ++i) {
        rowIndexCols.add(i);
      }
    }
    System.out.println("\nFile Statistics:");
    for (int i = 0; i < stats.length; ++i) {
      System.out.println("  Column " + i + ": " + stats[i].toString());
    }
    System.out.println("\nStripes:");
    int stripeIx = -1;
    for (StripeInformation stripe : reader.getStripes()) {
      ++stripeIx;
      long stripeStart = stripe.getOffset();
      OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
      if (printTimeZone) {
        String tz = footer.getWriterTimezone();
        if (tz == null || tz.isEmpty()) {
          tz = UNKNOWN;
        }
        System.out.println("  Stripe: " + stripe + " timezone: " + tz);
      } else {
        System.out.println("  Stripe: " + stripe);
      }
      long sectionStart = stripeStart;
      for (OrcProto.Stream section : footer.getStreamsList()) {
        String kind = section.hasKind() ? section.getKind().name() : UNKNOWN;
        System.out.println("    Stream: column " + section.getColumn() +
            " section " + kind + " start: " + sectionStart +
            " length " + section.getLength());
        sectionStart += section.getLength();
      }
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        StringBuilder buf = new StringBuilder();
        buf.append("    Encoding column ");
        buf.append(i);
        buf.append(": ");
        buf.append(encoding.getKind());
        if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          buf.append("[");
          buf.append(encoding.getDictionarySize());
          buf.append("]");
        }
        System.out.println(buf);
      }
      if (rowIndexCols != null && !rowIndexCols.isEmpty()) {
        // include the columns that are specified, only if the columns are included, bloom filter
        // will be read
        boolean[] sargColumns = new boolean[colCount];
        for (int colIdx : rowIndexCols) {
          sargColumns[colIdx] = true;
        }
        OrcIndex indices = rows.readRowIndex(stripeIx, null, sargColumns);
        for (int col : rowIndexCols) {
          StringBuilder buf = new StringBuilder();
          String rowIdxString = getFormattedRowIndices(col,
              indices.getRowGroupIndex(), schema, (ReaderImpl) reader);
          buf.append(rowIdxString);
          String bloomFilString = getFormattedBloomFilters(col, indices,
              reader.getWriterVersion(),
              reader.getSchema().findSubtype(col).getCategory(),
              footer.getColumns(col));
          buf.append(bloomFilString);
          System.out.println(buf);
        }
      }
    }

    FileSystem fs = file.getFileSystem(conf);
    long fileLen = fs.getFileStatus(file).getLen();
    long paddedBytes = getTotalPaddingSize(reader);
    double percentPadding = (fileLen == 0) ? 0.0d : 100.0d * paddedBytes / fileLen;
    DecimalFormat format = new DecimalFormat("##.##");
    System.out.println("\nFile length: " + fileLen + " bytes");
    System.out.println("Padding length: " + paddedBytes + " bytes");
    System.out.println("Padding ratio: " + format.format(percentPadding) + "%");
    //print out any user metadata properties
    List<String> keys = reader.getMetadataKeys();
    for(int i = 0; i < keys.size(); i++) {
      if(i == 0) {
        System.out.println("\nUser Metadata:");
      }
      ByteBuffer byteBuffer = reader.getMetadataValue(keys.get(i));
      System.out.println("  " + keys.get(i) + "="
          + StandardCharsets.UTF_8.decode(byteBuffer));
    }
    rows.close();
  }

  private static void recoverFiles(final List<String> corruptFiles, final Configuration conf,
      final String backup)
      throws IOException {

    byte[] magicBytes = OrcFile.MAGIC.getBytes(StandardCharsets.UTF_8);
    int magicLength = magicBytes.length;
    int readSize = conf.getInt(RECOVER_READ_SIZE, DEFAULT_BLOCK_SIZE);

    for (String corruptFile : corruptFiles) {
      System.err.println("Recovering file " + corruptFile);
      Path corruptPath = new Path(corruptFile);
      FileSystem fs = corruptPath.getFileSystem(conf);
      FSDataInputStream fdis = fs.open(corruptPath);
      try {
        long corruptFileLen = fs.getFileStatus(corruptPath).getLen();
        long remaining = corruptFileLen;
        List<Long> footerOffsets = new ArrayList<>();

        // start reading the data file form top to bottom and record the valid footers
        while (remaining > 0 && corruptFileLen > (2L * magicLength)) {
          int toRead = (int) Math.min(readSize, remaining);
          long startPos = corruptFileLen - remaining;
          byte[] data;
          if (startPos == 0) {
            data = new byte[toRead];
            fdis.readFully(startPos, data, 0, toRead);
          }
          else {
            // For non-first reads, we let startPos move back magicLength bytes
            // which prevents two adjacent reads from separating OrcFile.MAGIC
            startPos = startPos - magicLength;
            data = new byte[toRead + magicLength];
            fdis.readFully(startPos, data, 0, toRead + magicLength);
          }

          // find all MAGIC string and see if the file is readable from there
          int index = 0;
          long nextFooterOffset;
          while (index != -1) {
            // There are two reasons for searching from index + 1
            // 1. to skip the OrcFile.MAGIC in the file header when the first match is made
            // 2. When the match is successful, the index is moved backwards to search for
            // the subsequent OrcFile.MAGIC
            index = indexOf(data, magicBytes, index + 1);
            if (index != -1) {
              nextFooterOffset = startPos + index + magicBytes.length + 1;
              if (isReadable(corruptPath, conf, nextFooterOffset)) {
                footerOffsets.add(nextFooterOffset);
              }
            }
          }

          System.err.println("Scanning for valid footers - startPos: " + startPos +
              " toRead: " + toRead + " remaining: " + remaining);
          remaining = remaining - toRead;
        }

        System.err.println("Readable footerOffsets: " + footerOffsets);
        recoverFile(corruptPath, fs, conf, footerOffsets, backup);
      } catch (Exception e) {
        Path recoveryFile = getRecoveryFile(corruptPath);
        if (fs.exists(recoveryFile)) {
          fs.delete(recoveryFile, false);
        }
        System.err.println("Unable to recover file " + corruptFile);
        e.printStackTrace();
        System.err.println(SEPARATOR);
        continue;
      } finally {
        fdis.close();
      }
      System.err.println(corruptFile + " recovered successfully!");
      System.err.println(SEPARATOR);
    }
  }

  private static void recoverFile(final Path corruptPath, final FileSystem fs,
      final Configuration conf, final List<Long> footerOffsets, final String backup)
      throws IOException {

    // first recover the file to .recovered file and then once successful rename it to actual file
    Path recoveredPath = getRecoveryFile(corruptPath);

    // make sure that file does not exist
    if (fs.exists(recoveredPath)) {
      fs.delete(recoveredPath, false);
    }

    // if there are no valid footers, the file should still be readable so create an empty orc file
    if (footerOffsets == null || footerOffsets.isEmpty()) {
      System.err.println("No readable footers found. Creating empty orc file.");
      TypeDescription schema = TypeDescription.createStruct();
      Writer writer = OrcFile.createWriter(recoveredPath,
          OrcFile.writerOptions(conf).setSchema(schema));
      writer.close();
    } else {
      FSDataInputStream fdis = fs.open(corruptPath);
      FileStatus fileStatus = fs.getFileStatus(corruptPath);
      // read corrupt file and copy it to recovered file until last valid footer
      FSDataOutputStream fdos = fs.create(recoveredPath, true,
          conf.getInt("io.file.buffer.size", 4096),
          fileStatus.getReplication(),
          fileStatus.getBlockSize());
      try {
        long fileLen = footerOffsets.get(footerOffsets.size() - 1);
        long remaining = fileLen;

        while (remaining > 0) {
          int toRead = (int) Math.min(DEFAULT_BLOCK_SIZE, remaining);
          byte[] data = new byte[toRead];
          long startPos = fileLen - remaining;
          fdis.readFully(startPos, data, 0, toRead);
          fdos.write(data);
          System.err.println("Copying data to recovery file - startPos: " + startPos +
              " toRead: " + toRead + " remaining: " + remaining);
          remaining = remaining - toRead;
        }
      } catch (Exception e) {
        fs.delete(recoveredPath, false);
        throw new IOException(e);
      } finally {
        fdis.close();
        fdos.close();
      }
    }

    // validate the recovered file once again and start moving corrupt files to backup folder
    if (isReadable(recoveredPath, conf, Long.MAX_VALUE)) {
      Path backupDataPath;
      Path backupDirPath;
      Path relativeCorruptPath;
      String scheme = corruptPath.toUri().getScheme();
      String authority = corruptPath.toUri().getAuthority();

      // use the same filesystem as corrupt file if backup-path is not explicitly specified
      if (backup.equals(DEFAULT_BACKUP_PATH)) {
        backupDirPath = new Path(scheme, authority, DEFAULT_BACKUP_PATH);
      } else {
        backupDirPath = new Path(backup);
      }

      if (corruptPath.isUriPathAbsolute()) {
        relativeCorruptPath = corruptPath;
      } else {
        relativeCorruptPath = Path.mergePaths(new Path(Path.SEPARATOR), corruptPath);
      }

      backupDataPath = Path.mergePaths(backupDirPath, relativeCorruptPath);

      // Move data file to backup path
      moveFiles(fs, corruptPath, backupDataPath);

      // Move side file to backup path
      Path sideFilePath = OrcAcidUtils.getSideFile(corruptPath);
      if (fs.exists(sideFilePath)) {
        Path backupSideFilePath = new Path(backupDataPath.getParent(), sideFilePath.getName());
        moveFiles(fs, sideFilePath, backupSideFilePath);
      }

      // finally move recovered file to actual file
      moveFiles(fs, recoveredPath, corruptPath);

      // we are done recovering, backing up and validating
      System.err.println("Validation of recovered file successful!");
    }
  }

  private static void moveFiles(final FileSystem fs, final Path src, final Path dest)
      throws IOException {
    try {
      // create the dest directory if not exist
      if (!fs.exists(dest.getParent())) {
        fs.mkdirs(dest.getParent());
      }

      // if the destination file exists for some reason delete it
      fs.delete(dest, false);

      if (fs.rename(src, dest)) {
        System.err.println("Moved " + src + " to " + dest);
      } else {
        throw new IOException("Unable to move " + src + " to " + dest);
      }

    } catch (Exception e) {
      throw new IOException("Unable to move " + src + " to " + dest, e);
    }
  }

  private static Path getRecoveryFile(final Path corruptPath) {
    return new Path(corruptPath.getParent(), corruptPath.getName() + ".recovered");
  }

  private static boolean isReadable(final Path corruptPath, final Configuration conf,
      final long maxLen) {
    try {
      OrcFile.createReader(corruptPath, OrcFile.readerOptions(conf).maxLength(maxLen));
      return true;
    } catch (Exception e) {
      // ignore this exception as maxLen is unreadable
      return false;
    }
  }

  // search for byte pattern in another byte array
  public static int indexOf(final byte[] data, final byte[] pattern, final int index) {
    if (data == null || data.length == 0 || pattern == null || pattern.length == 0 ||
        index > data.length || index < 0) {
      return -1;
    }

    for (int i = index; i < data.length - pattern.length + 1; i++) {
      boolean found = true;
      for (int j = 0; j < pattern.length; j++) {
        if (data[i + j] != pattern[j]) {
          found = false;
          break;
        }
      }
      if (found) return i;
    }
    return -1;
  }

  private static String getFormattedBloomFilters(int col, OrcIndex index,
                                                 OrcFile.WriterVersion version,
                                                 TypeDescription.Category type,
                                                 OrcProto.ColumnEncoding encoding) {
    OrcProto.BloomFilterIndex[] bloomFilterIndex = index.getBloomFilterIndex();
    StringBuilder buf = new StringBuilder();
    BloomFilter stripeLevelBF = null;
    if (bloomFilterIndex != null && bloomFilterIndex[col] != null) {
      int idx = 0;
      buf.append("\n    Bloom filters for column ").append(col).append(":");
      for (OrcProto.BloomFilter bf : bloomFilterIndex[col].getBloomFilterList()) {
        BloomFilter toMerge = BloomFilterIO.deserialize(
            index.getBloomFilterKinds()[col], encoding, version, type, bf);
        buf.append("\n      Entry ").append(idx++).append(":").append(getBloomFilterStats(toMerge));
        if (stripeLevelBF == null) {
          stripeLevelBF = toMerge;
        } else {
          stripeLevelBF.merge(toMerge);
        }
      }
      String bloomFilterStats = getBloomFilterStats(stripeLevelBF);
      buf.append("\n      Stripe level merge:").append(bloomFilterStats);
    }
    return buf.toString();
  }

  private static String getBloomFilterStats(BloomFilter bf) {
    StringBuilder sb = new StringBuilder();
    int bitCount = bf.getBitSize();
    int popCount = 0;
    for (long l : bf.getBitSet()) {
      popCount += Long.bitCount(l);
    }
    int k = bf.getNumHashFunctions();
    float loadFactor = (float) popCount / (float) bitCount;
    float expectedFpp = (float) Math.pow(loadFactor, k);
    DecimalFormat df = new DecimalFormat("###.####");
    sb.append(" numHashFunctions: ").append(k);
    sb.append(" bitCount: ").append(bitCount);
    sb.append(" popCount: ").append(popCount);
    sb.append(" loadFactor: ").append(df.format(loadFactor));
    sb.append(" expectedFpp: ").append(expectedFpp);
    return sb.toString();
  }

  private static String getFormattedRowIndices(int col,
                                               OrcProto.RowIndex[] rowGroupIndex,
                                               TypeDescription schema,
                                               ReaderImpl reader) {
    StringBuilder buf = new StringBuilder();
    OrcProto.RowIndex index;
    buf.append("    Row group indices for column ").append(col).append(":");
    if (rowGroupIndex == null || (col >= rowGroupIndex.length) ||
        ((index = rowGroupIndex[col]) == null)) {
      buf.append(" not found\n");
      return buf.toString();
    }

    TypeDescription colSchema = schema.findSubtype(col);
    for (int entryIx = 0; entryIx < index.getEntryCount(); ++entryIx) {
      buf.append("\n      Entry ").append(entryIx).append(": ");
      OrcProto.RowIndexEntry entry = index.getEntry(entryIx);
      if (entry == null) {
        buf.append("unknown\n");
        continue;
      }
      if (!entry.hasStatistics()) {
        buf.append("no stats at ");
      } else {
        OrcProto.ColumnStatistics colStats = entry.getStatistics();
        ColumnStatistics cs =
            ColumnStatisticsImpl.deserialize(colSchema, colStats,
                reader.writerUsedProlepticGregorian(),
                reader.getConvertToProlepticGregorian());
        buf.append(cs);
      }
      buf.append(" positions: ");
      for (int posIx = 0; posIx < entry.getPositionsCount(); ++posIx) {
        if (posIx != 0) {
          buf.append(",");
        }
        buf.append(entry.getPositions(posIx));
      }
    }
    return buf.toString();
  }

  public static long getTotalPaddingSize(Reader reader) throws IOException {
    long paddedBytes = 0;
    List<StripeInformation> stripes = reader.getStripes();
    for (int i = 1; i < stripes.size(); i++) {
      long prevStripeOffset = stripes.get(i - 1).getOffset();
      long prevStripeLen = stripes.get(i - 1).getLength();
      paddedBytes += stripes.get(i).getOffset() - (prevStripeOffset + prevStripeLen);
    }
    return paddedBytes;
  }

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    // add -d and --data to print the rows
    result.addOption(Option.builder("d")
        .longOpt("data")
        .desc("Should the data be printed")
        .build());

    // to avoid breaking unit tests (when run in different time zones) for file dump, printing
    // of timezone is made optional
    result.addOption(Option.builder("t")
        .longOpt("timezone")
        .desc("Print writer's time zone")
        .build());

    result.addOption(Option.builder("h")
        .longOpt("help")
        .desc("Print help message")
        .build());

    result.addOption(Option.builder("r")
        .longOpt("rowindex")
        .argName("comma separated list of column ids for which row index should be printed")
        .desc("Dump stats for column number(s)")
        .hasArg()
        .build());

    result.addOption(Option.builder("j")
        .longOpt("json")
        .desc("Print metadata in JSON format")
        .build());

    result.addOption(Option.builder("p")
        .longOpt("pretty")
        .desc("Pretty print json metadata output")
        .build());

    result.addOption(Option.builder()
        .longOpt("recover")
        .desc("recover corrupted orc files generated by streaming")
        .build());

    result.addOption(Option.builder()
        .longOpt("skip-dump")
        .desc("used along with --recover to directly recover files without dumping")
        .build());

    result.addOption(Option.builder()
        .longOpt("backup-path")
        .desc("specify a backup path to store the corrupted files (default: /tmp)")
        .hasArg()
        .build());
    return result;
  }

}
