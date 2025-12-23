/**
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

#include "ToolsHelper.hh"

#include <getopt.h>

void printOptions(std::ostream& out) {
  out << "Options:\n"
      << "\t-h --help\n"
      << "\t-c --columns\t\tComma separated list of top-level column fields\n"
      << "\t-t --columnTypeIds\tComma separated list of column type ids\n"
      << "\t-n --columnNames\tComma separated list of column names\n"
      << "\t-b --batch\t\tBatch size for reading\n"
      << "\t-m --metrics\t\tShow metrics for reading\n";
}

bool parseOptions(int* argc, char** argv[], uint64_t* batchSize,
                  orc::RowReaderOptions* rowReaderOpts, bool* showMetrics) {
  static struct option longOptions[] = {{"help", no_argument, nullptr, 'h'},
                                        {"batch", required_argument, nullptr, 'b'},
                                        {"columns", required_argument, nullptr, 'c'},
                                        {"columnTypeIds", required_argument, nullptr, 't'},
                                        {"columnNames", required_argument, nullptr, 'n'},
                                        {"metrics", no_argument, nullptr, 'm'},
                                        {nullptr, 0, nullptr, 0}};
  std::list<uint64_t> cols;
  std::list<std::string> colNames;
  int opt;
  char* tail;
  do {
    opt = getopt_long(*argc, *argv, "hb:c:t:n:m", longOptions, nullptr);
    switch (opt) {
      case '?':
      case 'h':
        return false;
      case 'b':
        *batchSize = strtoul(optarg, &tail, 10);
        if (*tail != '\0') {
          fprintf(stderr, "The --batch parameter requires an integer option.\n");
          return false;
        }
        break;
      case 't':
      case 'c':
      case 'n': {
        bool empty = true;
        char* col = std::strtok(optarg, ",");
        while (col) {
          if (opt == 'n') {
            colNames.emplace_back(col);
          } else {
            cols.emplace_back(static_cast<uint64_t>(std::atoi(col)));
          }
          empty = false;
          col = std::strtok(nullptr, ",");
        }
        if (!empty) {
          if (opt == 'c') {
            rowReaderOpts->include(cols);
          } else if (opt == 't') {
            rowReaderOpts->includeTypes(cols);
          } else {
            rowReaderOpts->include(colNames);
          }
        }
        break;
      }
      case 'm': {
        *showMetrics = true;
        break;
      }
      default:
        break;
    }
  } while (opt != -1);
  *argc -= optind;
  *argv += optind;
  return true;
}

void printReaderMetrics(std::ostream& out, const orc::ReaderMetrics* metrics) {
  if (metrics != nullptr) {
    static const uint64_t US_PER_SECOND = 1000000;
    out << "ElapsedTimeSeconds: " << metrics->ReaderInclusiveLatencyUs / US_PER_SECOND << std::endl;
    out << "DecompressionLatencySeconds: " << metrics->DecompressionLatencyUs / US_PER_SECOND
        << std::endl;
    out << "DecodingLatencySeconds: " << metrics->DecodingLatencyUs / US_PER_SECOND << std::endl;
    out << "ByteDecodingLatencySeconds: " << metrics->ByteDecodingLatencyUs / US_PER_SECOND
        << std::endl;
    out << "IOBlockingLatencySeconds: " << metrics->IOBlockingLatencyUs / US_PER_SECOND
        << std::endl;
    out << "ReaderCall: " << metrics->ReaderCall << std::endl;
    out << "DecompressionCall: " << metrics->DecompressionCall << std::endl;
    out << "DecodingCall: " << metrics->DecodingCall << std::endl;
    out << "ByteDecodingCall: " << metrics->ByteDecodingCall << std::endl;
    out << "IOCount: " << metrics->IOCount << std::endl;
    out << "PPD SelectedRowGroupCount: " << metrics->SelectedRowGroupCount << std::endl;
    out << "PPD EvaluatedRowGroupCount: " << metrics->EvaluatedRowGroupCount << std::endl;
  }
}
