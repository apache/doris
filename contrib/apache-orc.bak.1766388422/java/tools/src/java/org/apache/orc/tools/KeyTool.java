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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.KeyProvider;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.security.SecureRandom;

/**
 * Print the information about the encryption keys.
 */
public class KeyTool {

  static void printKey(JsonWriter writer,
                       KeyProvider provider,
                       String keyName) throws IOException {
    HadoopShims.KeyMetadata meta = provider.getCurrentKeyVersion(keyName);
    writer.beginObject();
    writer.name("name");
    writer.value(keyName);
    EncryptionAlgorithm algorithm = meta.getAlgorithm();
    writer.name("algorithm");
    writer.value(algorithm.getAlgorithm());
    writer.name("keyLength");
    writer.value(algorithm.keyLength());
    writer.name("version");
    writer.value(meta.getVersion());
    byte[] iv = new byte[algorithm.getIvLength()];
    byte[] key = provider.decryptLocalKey(meta, iv).getEncoded();
    writer.name("key 0");
    writer.value(new BytesWritable(key).toString());
    writer.endObject();
  }

  private final OutputStreamWriter writer;
  private final Configuration conf;

  public KeyTool(Configuration conf,
                 String[] args) throws IOException, ParseException {
    CommandLine opts = parseOptions(args);
    PrintStream stream;
    if (opts.hasOption('o')) {
      stream = new PrintStream(opts.getOptionValue('o'), "UTF-8");
    } else {
      stream = System.out;
    }
    writer = new OutputStreamWriter(stream, "UTF-8");
    this.conf = conf;
  }

  void run() throws IOException {
    KeyProvider provider =
        CryptoUtils.getKeyProvider(conf, new SecureRandom());
    if (provider == null) {
      System.err.println("No key provider available.");
      System.exit(1);
    }
    for(String keyName: provider.getKeyNames()) {
      JsonWriter writer = new JsonWriter(this.writer);
      printKey(writer, provider, keyName);
      this.writer.write('\n');
    }
    this.writer.close();
  }

  private static CommandLine parseOptions(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption(
        Option.builder("h").longOpt("help").desc("Provide help").build());
    options.addOption(
        Option.builder("o").longOpt("output").desc("Output filename")
            .hasArg().build());
    CommandLine cli = new DefaultParser().parse(options, args);
    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("key", options);
      System.exit(1);
    }
    return cli;
  }

  public static void main(Configuration conf,
                          String[] args
                          ) throws IOException, ParseException {
    KeyTool tool = new KeyTool(conf, args);
    tool.run();
  }
}
