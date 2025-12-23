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

#include "Timezone.hh"
#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"

#include <getopt.h>
#include <sys/time.h>
#include <time.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <string>

static char gDelimiter = ',';

// extract one column raw text from one line
std::string extractColumn(std::string s, uint64_t colIndex) {
  uint64_t col = 0;
  size_t start = 0;
  size_t end = s.find(gDelimiter);
  while (col < colIndex && end != std::string::npos) {
    start = end + 1;
    end = s.find(gDelimiter, start);
    ++col;
  }
  return col == colIndex ? s.substr(start, end - start) : "";
}

static const char* GetDate(void) {
  static char buf[200];
  time_t t = time(nullptr);
  struct tm* p = localtime(&t);
  strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", p);
  return buf;
}

void fillLongValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                    uint64_t numValues, uint64_t colIndex) {
  orc::LongVectorBatch* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      batch->notNull[i] = 1;
      longBatch->data[i] = atoll(col.c_str());
    }
  }
  longBatch->hasNulls = hasNull;
  longBatch->numElements = numValues;
}

void fillStringValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                      uint64_t numValues, uint64_t colIndex, orc::DataBuffer<char>& buffer) {
  uint64_t offset = 0;
  orc::StringVectorBatch* stringBatch = dynamic_cast<orc::StringVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      batch->notNull[i] = 1;
      char* oldBufferAddress = buffer.data();
      // Resize the buffer in case buffer does not have remaining space to store the next string.
      while (buffer.size() - offset < col.size()) {
        buffer.resize(buffer.size() * 2);
      }
      char* newBufferAddress = buffer.data();
      // Refill stringBatch->data with the new addresses, if buffer's address has changed.
      if (newBufferAddress != oldBufferAddress) {
        for (uint64_t refillIndex = 0; refillIndex < i; ++refillIndex) {
          stringBatch->data[refillIndex] =
              stringBatch->data[refillIndex] - oldBufferAddress + newBufferAddress;
        }
      }
      memcpy(buffer.data() + offset, col.c_str(), col.size());
      stringBatch->data[i] = buffer.data() + offset;
      stringBatch->length[i] = static_cast<int64_t>(col.size());
      offset += col.size();
    }
  }
  stringBatch->hasNulls = hasNull;
  stringBatch->numElements = numValues;
}

void fillDoubleValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                      uint64_t numValues, uint64_t colIndex) {
  orc::DoubleVectorBatch* dblBatch = dynamic_cast<orc::DoubleVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      batch->notNull[i] = 1;
      dblBatch->data[i] = atof(col.c_str());
    }
  }
  dblBatch->hasNulls = hasNull;
  dblBatch->numElements = numValues;
}

// parse fixed point decimal numbers
void fillDecimalValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                       uint64_t numValues, uint64_t colIndex, size_t scale, size_t precision) {
  orc::Decimal128VectorBatch* d128Batch = nullptr;
  orc::Decimal64VectorBatch* d64Batch = nullptr;
  if (precision <= 18) {
    d64Batch = dynamic_cast<orc::Decimal64VectorBatch*>(batch);
    d64Batch->scale = static_cast<int32_t>(scale);
  } else {
    d128Batch = dynamic_cast<orc::Decimal128VectorBatch*>(batch);
    d128Batch->scale = static_cast<int32_t>(scale);
  }
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      batch->notNull[i] = 1;
      size_t ptPos = col.find('.');
      size_t curScale = 0;
      std::string num = col;
      if (ptPos != std::string::npos) {
        curScale = col.length() - ptPos - 1;
        num = col.substr(0, ptPos) + col.substr(ptPos + 1);
      }
      orc::Int128 decimal(num);
      while (curScale != scale) {
        curScale++;
        decimal *= 10;
      }
      if (precision <= 18) {
        d64Batch->values[i] = decimal.toLong();
      } else {
        d128Batch->values[i] = decimal;
      }
    }
  }
  batch->hasNulls = hasNull;
  batch->numElements = numValues;
}

void fillBoolValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                    uint64_t numValues, uint64_t colIndex) {
  orc::LongVectorBatch* boolBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      batch->notNull[i] = 1;
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      if (col == "true" || col == "t") {
        boolBatch->data[i] = true;
      } else {
        boolBatch->data[i] = false;
      }
    }
  }
  boolBatch->hasNulls = hasNull;
  boolBatch->numElements = numValues;
}

// parse date string from format YYYY-mm-dd
void fillDateValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                    uint64_t numValues, uint64_t colIndex) {
  orc::LongVectorBatch* longBatch = dynamic_cast<orc::LongVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      batch->notNull[i] = 1;
      struct tm tm;
      memset(&tm, 0, sizeof(struct tm));
      strptime(col.c_str(), "%Y-%m-%d", &tm);
      time_t t = mktime(&tm);
      time_t t1970 = 0;
      double seconds = difftime(t, t1970);
      int64_t days = static_cast<int64_t>(seconds / (60 * 60 * 24));
      longBatch->data[i] = days;
    }
  }
  longBatch->hasNulls = hasNull;
  longBatch->numElements = numValues;
}

// parse timestamp values in seconds
void fillTimestampValues(const std::vector<std::string>& data, orc::ColumnVectorBatch* batch,
                         uint64_t numValues, uint64_t colIndex) {
  struct tm timeStruct;
  orc::TimestampVectorBatch* tsBatch = dynamic_cast<orc::TimestampVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      memset(&timeStruct, 0, sizeof(timeStruct));
      char* left = strptime(col.c_str(), "%Y-%m-%d %H:%M:%S", &timeStruct);
      if (left == nullptr) {
        batch->notNull[i] = 0;
      } else {
        batch->notNull[i] = 1;
        tsBatch->data[i] = timegm(&timeStruct);
        char* tail;
        double d = strtod(left, &tail);
        if (tail != left) {
          tsBatch->nanoseconds[i] = static_cast<long>(d * 1000000000.0);
        } else {
          tsBatch->nanoseconds[i] = 0;
        }
      }
    }
  }
  tsBatch->hasNulls = hasNull;
  tsBatch->numElements = numValues;
}

void usage() {
  std::cout << "Usage: csv-import [-h] [--help]\n"
            << "                  [-m] [--metrics]\n"
            << "                  [-d <character>] [--delimiter=<character>]\n"
            << "                  [-s <size>] [--stripe=<size>]\n"
            << "                  [-c <size>] [--block=<size>]\n"
            << "                  [-b <size>] [--batch=<size>]\n"
            << "                  [-t <string>] [--timezone=<string>]\n"
            << "                  <schema> <input> <output>\n"
            << "Import CSV file into an Orc file using the specified schema.\n"
            << "The timezone is writer timezone of timestamp types.\n"
            << "Compound types are not yet supported.\n";
}

int main(int argc, char* argv[]) {
  std::string input;
  std::string output;
  std::string schema;
  std::string timezoneName = "GMT";
  uint64_t stripeSize = (128 << 20);  // 128M
  uint64_t blockSize = 64 << 10;      // 64K
  uint64_t batchSize = 1024;
  orc::CompressionKind compression = orc::CompressionKind_ZLIB;

  static struct option longOptions[] = {{"help", no_argument, nullptr, 'h'},
                                        {"metrics", no_argument, nullptr, 'm'},
                                        {"delimiter", required_argument, nullptr, 'd'},
                                        {"stripe", required_argument, nullptr, 's'},
                                        {"block", required_argument, nullptr, 'c'},
                                        {"batch", required_argument, nullptr, 'b'},
                                        {"timezone", required_argument, nullptr, 't'},
                                        {nullptr, 0, nullptr, 0}};
  bool helpFlag = false;
  bool showMetrics = false;
  int opt;
  char* tail;
  do {
    opt = getopt_long(argc, argv, "d:s:c:b:t:mh", longOptions, nullptr);
    switch (opt) {
      case 'h':
        helpFlag = true;
        opt = -1;
        break;
      case 'm':
        showMetrics = true;
        break;
      case 'd':
        gDelimiter = optarg[0];
        break;
      case 's':
        stripeSize = strtoul(optarg, &tail, 10);
        if (*tail != '\0') {
          fprintf(stderr, "The --stripe parameter requires an integer option.\n");
          return 1;
        }
        break;
      case 'c':
        blockSize = strtoul(optarg, &tail, 10);
        if (*tail != '\0') {
          fprintf(stderr, "The --block parameter requires an integer option.\n");
          return 1;
        }
        break;
      case 'b':
        batchSize = strtoul(optarg, &tail, 10);
        if (*tail != '\0') {
          fprintf(stderr, "The --batch parameter requires an integer option.\n");
          return 1;
        }
        break;
      case 't':
        timezoneName = std::string(optarg);
        break;
    }
  } while (opt != -1);

  argc -= optind;
  argv += optind;

  if (argc != 3 || helpFlag) {
    usage();
    return 1;
  }

  schema = argv[0];
  input = argv[1];
  output = argv[2];

  std::cout << GetDate() << " Start importing Orc file..." << std::endl;
  std::unique_ptr<orc::Type> fileType = orc::Type::buildTypeFromString(schema);

  double totalElapsedTime = 0.0;
  clock_t totalCPUTime = 0;

  typedef std::list<orc::DataBuffer<char>> DataBufferList;
  DataBufferList bufferList;

  orc::WriterOptions options;
  orc::WriterMetrics metrics;
  options.setStripeSize(stripeSize);
  options.setCompressionBlockSize(blockSize);
  options.setCompression(compression);
  options.setTimezoneName(timezoneName);
  options.setWriterMetrics(showMetrics ? &metrics : nullptr);

  std::unique_ptr<orc::OutputStream> outStream = orc::writeLocalFile(output);
  std::unique_ptr<orc::Writer> writer = orc::createWriter(*fileType, outStream.get(), options);
  std::unique_ptr<orc::ColumnVectorBatch> rowBatch = writer->createRowBatch(batchSize);

  bool eof = false;
  std::string line;
  std::vector<std::string> data;  // buffer that holds a batch of rows in raw text
  std::ifstream finput(input.c_str());
  while (!eof) {
    uint64_t numValues = 0;  // num of lines read in a batch

    data.clear();
    memset(rowBatch->notNull.data(), 1, batchSize);

    // read a batch of lines from the input file
    for (uint64_t i = 0; i < batchSize; ++i) {
      if (!std::getline(finput, line)) {
        eof = true;
        break;
      }
      data.push_back(line);
      ++numValues;
    }

    if (numValues != 0) {
      orc::StructVectorBatch* structBatch = dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
      structBatch->numElements = numValues;

      for (uint64_t i = 0; i < structBatch->fields.size(); ++i) {
        const orc::Type* subType = fileType->getSubtype(i);
        switch (subType->getKind()) {
          case orc::BYTE:
          case orc::INT:
          case orc::SHORT:
          case orc::LONG:
            fillLongValues(data, structBatch->fields[i], numValues, i);
            break;
          case orc::STRING:
          case orc::CHAR:
          case orc::VARCHAR:
          case orc::BINARY:
            bufferList.emplace_back(*orc::getDefaultPool(), 1 * 1024 * 1024);
            fillStringValues(data, structBatch->fields[i], numValues, i, bufferList.back());
            break;
          case orc::FLOAT:
          case orc::DOUBLE:
            fillDoubleValues(data, structBatch->fields[i], numValues, i);
            break;
          case orc::DECIMAL:
            fillDecimalValues(data, structBatch->fields[i], numValues, i, subType->getScale(),
                              subType->getPrecision());
            break;
          case orc::BOOLEAN:
            fillBoolValues(data, structBatch->fields[i], numValues, i);
            break;
          case orc::DATE:
            fillDateValues(data, structBatch->fields[i], numValues, i);
            break;
          case orc::TIMESTAMP:
          case orc::TIMESTAMP_INSTANT:
            fillTimestampValues(data, structBatch->fields[i], numValues, i);
            break;
          case orc::STRUCT:
          case orc::LIST:
          case orc::MAP:
          case orc::UNION:
            throw std::runtime_error(subType->toString() + " is not supported yet.");
        }
      }

      struct timeval t_start, t_end;
      gettimeofday(&t_start, nullptr);
      clock_t c_start = clock();

      writer->add(*rowBatch);

      totalCPUTime += (clock() - c_start);
      gettimeofday(&t_end, nullptr);
      totalElapsedTime += (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0 +
                           static_cast<double>(t_end.tv_usec - t_start.tv_usec)) /
                          1000000.0;
    }
  }

  struct timeval t_start, t_end;
  gettimeofday(&t_start, nullptr);
  clock_t c_start = clock();

  writer->close();

  totalCPUTime += (clock() - c_start);
  gettimeofday(&t_end, nullptr);
  totalElapsedTime += (static_cast<double>(t_end.tv_sec - t_start.tv_sec) * 1000000.0 +
                       static_cast<double>(t_end.tv_usec - t_start.tv_usec)) /
                      1000000.0;

  std::cout << GetDate() << " Finish importing Orc file." << std::endl;
  std::cout << GetDate() << " Total writer elasped time: " << totalElapsedTime << "s." << std::endl;
  std::cout << GetDate()
            << " Total writer CPU time: " << static_cast<double>(totalCPUTime) / CLOCKS_PER_SEC
            << "s." << std::endl;
  if (showMetrics) {
    std::cout << GetDate() << " IO block lantency: "
              << static_cast<double>(metrics.IOBlockingLatencyUs) / 1000000.0 << "s." << std::endl;
    std::cout << GetDate() << " IO count: " << metrics.IOCount << std::endl;
  }
  return 0;
}
