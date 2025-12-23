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
package org.apache.orc.tools.convert;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.tools.json.JsonSchemaFinder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * A conversion tool to convert CSV or JSON files into ORC files.
 */
public class ConvertTool {
  static final String DEFAULT_TIMESTAMP_FORMAT =
      "yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[ ][XXX][X]";

  private final List<FileInformation> fileList;
  private final TypeDescription schema;
  private final char csvSeparator;
  private final char csvQuote;
  private final char csvEscape;
  private final int csvHeaderLines;
  private final String csvNullString;
  private final String timestampFormat;
  private final String bloomFilterColumns;
  private final String unionTag;
  private final String unionValue;
  private final Writer writer;
  private final VectorizedRowBatch batch;

  TypeDescription buildSchema(List<FileInformation> files,
                              Configuration conf) throws IOException {
    JsonSchemaFinder schemaFinder = new JsonSchemaFinder();
    int filesScanned = 0;
    for(FileInformation file: files) {
      if (file.format == Format.JSON) {
        System.err.println("Scanning " + file.path + " for schema");
        filesScanned += 1;
        schemaFinder.addFile(file.getReader(file.filesystem.open(file.path)), file.path.getName());
      } else if (file.format == Format.ORC) {
        System.err.println("Merging schema from " + file.path);
        filesScanned += 1;
        Reader reader = OrcFile.createReader(file.path,
            OrcFile.readerOptions(conf)
                .filesystem(file.filesystem));
        if (files.size() == 1) {
          return reader.getSchema();
        }
        schemaFinder.addSchema(reader.getSchema());
      }
    }
    if (filesScanned == 0) {
      throw new IllegalArgumentException("Please specify a schema using" +
          " --schema for converting CSV files.");
    }
    return schemaFinder.getSchema();
  }

  enum Compression {
    NONE, GZIP
  }

  enum Format {
    JSON, CSV, ORC
  }

  class FileInformation {
    private final Compression compression;
    private final Format format;
    private final Path path;
    private final FileSystem filesystem;
    private final Configuration conf;
    private final long size;

    FileInformation(Path path, Configuration conf) throws IOException {
      this.path = path;
      this.conf = conf;
      this.filesystem = path.getFileSystem(conf);
      this.size = filesystem.getFileStatus(path).getLen();
      String name = path.getName();
      int lastDot = name.lastIndexOf(".");
      if (lastDot >= 0 && ".gz".equals(name.substring(lastDot))) {
        this.compression = Compression.GZIP;
        name = name.substring(0, lastDot);
        lastDot = name.lastIndexOf(".");
      } else {
        this.compression = Compression.NONE;
      }
      if (lastDot >= 0) {
        String ext = name.substring(lastDot);
        if (".json".equals(ext) || ".jsn".equals(ext)) {
          format = Format.JSON;
        } else if (".csv".equals(ext)) {
          format = Format.CSV;
        } else if (".orc".equals(ext)) {
          format = Format.ORC;
        } else {
          throw new IllegalArgumentException("Unknown kind of file " + path);
        }
      } else {
        throw new IllegalArgumentException("No extension on file " + path);
      }
    }

    java.io.Reader getReader(InputStream input) throws IOException {
      if (compression == Compression.GZIP) {
        input = new GZIPInputStream(input);
      }
      return new InputStreamReader(input, StandardCharsets.UTF_8);
    }

    public RecordReader getRecordReader() throws IOException {
      switch (format) {
        case ORC: {
          Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
          return reader.rows(reader.options().schema(schema));
        }
        case JSON: {
          FSDataInputStream underlying = filesystem.open(path);
          return new JsonReader(getReader(underlying), underlying, size, schema, timestampFormat,
              unionTag, unionValue);
        }
        case CSV: {
          FSDataInputStream underlying = filesystem.open(path);
          return new CsvReader(getReader(underlying), underlying, size, schema,
              csvSeparator, csvQuote, csvEscape, csvHeaderLines, csvNullString, timestampFormat);
        }
        default:
          throw new IllegalArgumentException("Unhandled format " + format +
              " for " + path);
      }
    }
  }

  public static void main(Configuration conf,
                          String[] args) throws IOException, ParseException {
    new ConvertTool(conf, args).run();
  }


  List<FileInformation> buildFileList(String[] files,
                                      Configuration conf) throws IOException {
    List<FileInformation> result = new ArrayList<>(files.length);
    for(String fn: files) {
      result.add(new FileInformation(new Path(fn), conf));
    }
    return result;
  }

  public ConvertTool(Configuration conf,
                     String[] args) throws IOException, ParseException {
    CommandLine opts = parseOptions(args);
    fileList = buildFileList(opts.getArgs(), conf);
    if (opts.hasOption('s')) {
      this.schema = TypeDescription.fromString(opts.getOptionValue('s'));
    } else {
      this.schema = buildSchema(fileList, conf);
    }
    this.csvQuote = getCharOption(opts, 'q', '"');
    this.csvEscape = getCharOption(opts, 'e', '\\');
    this.csvSeparator = getCharOption(opts, 'S', ',');
    this.csvHeaderLines = getIntOption(opts, 'H', 0);
    this.csvNullString = opts.getOptionValue('n', "");
    this.timestampFormat = opts.getOptionValue("t", DEFAULT_TIMESTAMP_FORMAT);
    this.bloomFilterColumns = opts.getOptionValue('b', null);
    this.unionTag = opts.getOptionValue("union-tag", "tag");
    this.unionValue = opts.getOptionValue("union-value", "value");
    String outFilename = opts.hasOption('o')
        ? opts.getOptionValue('o') : "output.orc";
    boolean overwrite = opts.hasOption('O');
    OrcFile.WriterOptions writerOpts = OrcFile.writerOptions(conf)
        .setSchema(schema)
        .overwrite(overwrite);
    if (this.bloomFilterColumns != null) {
      writerOpts.bloomFilterColumns(this.bloomFilterColumns);
    }
    writer = OrcFile.createWriter(new Path(outFilename), writerOpts);
    batch = schema.createRowBatch();
  }

  void run() throws IOException {
    for (FileInformation file: fileList) {
      System.err.println("Processing " + file.path);
      RecordReader reader = file.getRecordReader();
      while (reader.nextBatch(batch)) {
        writer.addRowBatch(batch);
      }
      reader.close();
    }
    writer.close();
  }

  private static int getIntOption(CommandLine opts, char letter, int mydefault) {
    if (opts.hasOption(letter)) {
      return Integer.parseInt(opts.getOptionValue(letter));
    } else {
      return mydefault;
    }
  }

  private static char getCharOption(CommandLine opts, char letter, char mydefault) {
    if (opts.hasOption(letter)) {
      return opts.getOptionValue(letter).charAt(0);
    } else {
      return mydefault;
    }
  }

  private static CommandLine parseOptions(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption(
        Option.builder("h").longOpt("help").desc("Provide help").build());
    options.addOption(
        Option.builder("s").longOpt("schema").hasArg()
            .desc("The schema to write in to the file").build());
    options.addOption(
        Option.builder("b").longOpt("bloomFilterColumns").hasArg()
            .desc("Comma separated values of column names for which bloom filter is " +
                "to be created").build());
    options.addOption(
        Option.builder("o").longOpt("output").desc("Output filename")
            .hasArg().build());
    options.addOption(
        Option.builder("n").longOpt("null").desc("CSV null string")
            .hasArg().build());
    options.addOption(
        Option.builder("q").longOpt("quote").desc("CSV quote character")
            .hasArg().build());
    options.addOption(
        Option.builder("e").longOpt("escape").desc("CSV escape character")
            .hasArg().build());
    options.addOption(
        Option.builder("S").longOpt("separator").desc("CSV separator character")
            .hasArg().build());
    options.addOption(
        Option.builder("H").longOpt("header").desc("CSV header lines")
            .hasArg().build());
    options.addOption(
        Option.builder("t").longOpt("timestampformat").desc("Timestamp Format")
            .hasArg().build());
    options.addOption(
        Option.builder("O").longOpt("overwrite").desc("Overwrite an existing file")
            .build()
    );
    options.addOption(
        Option.builder().longOpt("union-tag")
            .desc("JSON key name representing UNION tag. Default to \"tag\".")
            .hasArg().build());
    options.addOption(
        Option.builder().longOpt("union-value")
            .desc("JSON key name representing UNION value. Default to \"value\".")
            .hasArg().build());
    CommandLine cli = new DefaultParser().parse(options, args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("convert", options);
      System.exit(1);
    }
    return cli;
  }
}
