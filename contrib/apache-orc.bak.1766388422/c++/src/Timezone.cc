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
#include "orc/OrcFile.hh"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>
#include <map>
#include <sstream>

namespace orc {

  // default location of the timezone files
  static const char DEFAULT_TZDIR[] = "/usr/share/zoneinfo";

  // location of a symlink to the local timezone
  static const char LOCAL_TIMEZONE[] = "/etc/localtime";

  enum TransitionKind { TRANSITION_JULIAN, TRANSITION_DAY, TRANSITION_MONTH };

  static const int64_t MONTHS_PER_YEAR = 12;
  /**
   * The number of days in each month in non-leap and leap years.
   */
  static const int64_t DAYS_PER_MONTH[2][MONTHS_PER_YEAR] = {
      {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
      {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
  static const int64_t DAYS_PER_WEEK = 7;

  // Leap years and day of the week repeat every 400 years, which makes it
  // a good cycle length.
  static const int64_t SECONDS_PER_400_YEARS =
      SECONDS_PER_DAY * (365 * (300 + 3) + 366 * (100 - 3));

  /**
   * Is the given year a leap year?
   */
  bool isLeap(int64_t year) {
    return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
  }

  /**
   * Find the position that is the closest and less than or equal to the
   * target.
   * @return -1 if the target < array[0] or array is empty or
   *          i if array[i] <= target and (i == n or array[i] < array[i+1])
   */
  int64_t binarySearch(const std::vector<int64_t>& array, int64_t target) {
    uint64_t size = array.size();
    if (size == 0) {
      return -1;
    }
    uint64_t min = 0;
    uint64_t max = size - 1;
    uint64_t mid = (min + max) / 2;
    while ((array[mid] != target) && (min < max)) {
      if (array[mid] < target) {
        min = mid + 1;
      } else if (mid == 0) {
        max = 0;
      } else {
        max = mid - 1;
      }
      mid = (min + max) / 2;
    }
    if (target < array[mid]) {
      return static_cast<int64_t>(mid) - 1;
    } else {
      return static_cast<int64_t>(mid);
    }
  }

  struct Transition {
    TransitionKind kind;
    int64_t day;
    int64_t week;
    int64_t month;
    int64_t time;

    std::string toString() const {
      std::stringstream buffer;
      switch (kind) {
        case TRANSITION_JULIAN:
          buffer << "julian " << day;
          break;
        case TRANSITION_DAY:
          buffer << "day " << day;
          break;
        case TRANSITION_MONTH:
          buffer << "month " << month << " week " << week << " day " << day;
          break;
      }
      buffer << " at " << (time / (60 * 60)) << ":" << ((time / 60) % 60) << ":" << (time % 60);
      return buffer.str();
    }

    /**
     * Get the transition time for the given year.
     * @param year the year
     * @return the number of seconds past local Jan 1 00:00:00 that the
     *    transition happens.
     */
    int64_t getTime(int64_t year) const {
      int64_t result = time;
      switch (kind) {
        case TRANSITION_JULIAN:
          result += SECONDS_PER_DAY * day;
          if (day > 60 && isLeap(year)) {
            result += SECONDS_PER_DAY;
          }
          break;
        case TRANSITION_DAY:
          result += SECONDS_PER_DAY * day;
          break;
        case TRANSITION_MONTH: {
          bool inLeap = isLeap(year);
          int64_t adjustedMonth = (month + 9) % 12 + 1;
          int64_t adjustedYear = (month <= 2) ? (year - 1) : year;
          int64_t adjustedCentury = adjustedYear / 100;
          int64_t adjustedRemainder = adjustedYear % 100;

          // day of the week of the first day of month
          int64_t dayOfWeek = ((26 * adjustedMonth - 2) / 10 + 1 + adjustedRemainder +
                               adjustedRemainder / 4 + adjustedCentury / 4 - 2 * adjustedCentury) %
                              7;
          if (dayOfWeek < 0) {
            dayOfWeek += DAYS_PER_WEEK;
          }

          int64_t d = day - dayOfWeek;
          if (d < 0) {
            d += DAYS_PER_WEEK;
          }
          for (int w = 1; w < week; ++w) {
            if (d + DAYS_PER_WEEK >= DAYS_PER_MONTH[inLeap][month - 1]) {
              break;
            }
            d += DAYS_PER_WEEK;
          }
          result += d * SECONDS_PER_DAY;

          // Add in the time for the month
          for (int m = 0; m < month - 1; ++m) {
            result += DAYS_PER_MONTH[inLeap][m] * SECONDS_PER_DAY;
          }
          break;
        }
      }
      return result;
    }
  };

  /**
   * The current rule for finding timezone variants arbitrarily far in
   * the future.  They are based on a string representation that
   * specifies the standard name and offset. For timezones with
   * daylight savings, the string specifies the daylight variant name
   * and offset and the rules for switching between them.
   *
   * rule = <standard name><standard offset><daylight>?
   * name = string with no numbers or '+', '-', or ','
   * offset = [-+]?hh(:mm(:ss)?)?
   * daylight = <name><offset>,<start day>(/<offset>)?,<end day>(/<offset>)?
   * day = J<day without 2/29>|<day with 2/29>|M<month>.<week>.<day of week>
   */
  class FutureRuleImpl : public FutureRule {
    std::string ruleString;
    TimezoneVariant standard;
    bool hasDst;
    TimezoneVariant dst;
    Transition start;
    Transition end;

    // expanded time_t offsets of transitions
    std::vector<int64_t> offsets;

    // Is the epoch (1 Jan 1970 00:00) in standard time?
    // This code assumes that the transition dates fall in the same order
    // each year. Hopefully no timezone regions decide to move across the
    // equator, which is about what it would take.
    bool startInStd;

    void computeOffsets() {
      if (!hasDst) {
        startInStd = true;
        offsets.resize(1);
      } else {
        // Insert a transition for the epoch and two per a year for the next
        // 400 years. We assume that the all even positions are in standard
        // time if and only if startInStd and the odd ones are the reverse.
        offsets.resize(400 * 2 + 1);
        startInStd = start.getTime(1970) < end.getTime(1970);
        int64_t base = 0;
        for (int64_t year = 1970; year < 1970 + 400; ++year) {
          if (startInStd) {
            offsets[static_cast<uint64_t>(year - 1970) * 2 + 1] =
                base + start.getTime(year) - standard.gmtOffset;
            offsets[static_cast<uint64_t>(year - 1970) * 2 + 2] =
                base + end.getTime(year) - dst.gmtOffset;
          } else {
            offsets[static_cast<uint64_t>(year - 1970) * 2 + 1] =
                base + end.getTime(year) - dst.gmtOffset;
            offsets[static_cast<uint64_t>(year - 1970) * 2 + 2] =
                base + start.getTime(year) - standard.gmtOffset;
          }
          base += (isLeap(year) ? 366 : 365) * SECONDS_PER_DAY;
        }
      }
      offsets[0] = 0;
    }

   public:
    virtual ~FutureRuleImpl() override;
    bool isDefined() const override;
    const TimezoneVariant& getVariant(int64_t clk) const override;
    void print(std::ostream& out) const override;

    friend class FutureRuleParser;
  };

  FutureRule::~FutureRule() {
    // PASS
  }

  FutureRuleImpl::~FutureRuleImpl() {
    // PASS
  }

  bool FutureRuleImpl::isDefined() const {
    return ruleString.size() > 0;
  }

  const TimezoneVariant& FutureRuleImpl::getVariant(int64_t clk) const {
    if (!hasDst) {
      return standard;
    } else {
      int64_t adjusted = clk % SECONDS_PER_400_YEARS;
      if (adjusted < 0) {
        adjusted += SECONDS_PER_400_YEARS;
      }
      int64_t idx = binarySearch(offsets, adjusted);
      if (startInStd == (idx % 2 == 0)) {
        return standard;
      } else {
        return dst;
      }
    }
  }

  void FutureRuleImpl::print(std::ostream& out) const {
    if (isDefined()) {
      out << "  Future rule: " << ruleString << "\n";
      out << "  standard " << standard.toString() << "\n";
      if (hasDst) {
        out << "  dst " << dst.toString() << "\n";
        out << "  start " << start.toString() << "\n";
        out << "  end " << end.toString() << "\n";
      }
    }
  }

  /**
   * A parser for the future rule strings.
   */
  class FutureRuleParser {
   public:
    FutureRuleParser(const std::string& str, FutureRuleImpl* rule)
        : ruleString(str), length(str.size()), position(0), output(*rule) {
      output.ruleString = str;
      if (position != length) {
        parseName(output.standard.name);
        output.standard.gmtOffset = -parseOffset();
        output.standard.isDst = false;
        output.hasDst = position < length;
        if (output.hasDst) {
          parseName(output.dst.name);
          output.dst.isDst = true;
          if (ruleString[position] != ',') {
            output.dst.gmtOffset = -parseOffset();
          } else {
            output.dst.gmtOffset = output.standard.gmtOffset + 60 * 60;
          }
          parseTransition(output.start);
          parseTransition(output.end);
        }
        if (position != length) {
          throwError("Extra text");
        }
        output.computeOffsets();
      }
    }

   private:
    const std::string& ruleString;
    size_t length;
    size_t position;
    FutureRuleImpl& output;

    void throwError(const char* msg) {
      std::stringstream buffer;
      buffer << msg << " at " << position << " in '" << ruleString << "'";
      throw TimezoneError(buffer.str());
    }

    /**
     * Parse the names of the form:
     *    ([^-+0-9,]+|<[^>]+>)
     * and set the output string.
     */
    void parseName(std::string& result) {
      if (position == length) {
        throwError("name required");
      }
      size_t start = position;
      if (ruleString[position] == '<') {
        while (position < length && ruleString[position] != '>') {
          position += 1;
        }
        if (position == length) {
          throwError("missing close '>'");
        }
        position += 1;
      } else {
        while (position < length) {
          char ch = ruleString[position];
          if (isdigit(ch) || ch == '-' || ch == '+' || ch == ',') {
            break;
          }
          position += 1;
        }
      }
      if (position == start) {
        throwError("empty string not allowed");
      }
      result = ruleString.substr(start, position - start);
    }

    /**
     * Parse an integer of the form [0-9]+ and return it.
     */
    int64_t parseNumber() {
      if (position >= length) {
        throwError("missing number");
      }
      int64_t result = 0;
      while (position < length) {
        char ch = ruleString[position];
        if (isdigit(ch)) {
          result = result * 10 + (ch - '0');
          position += 1;
        } else {
          break;
        }
      }
      return result;
    }

    /**
     * Parse the offsets of the form:
     *    [-+]?[0-9]+(:[0-9]+(:[0-9]+)?)?
     * and convert it into a number of seconds.
     */
    int64_t parseOffset() {
      int64_t scale = 3600;
      bool isNegative = false;
      if (position < length) {
        char ch = ruleString[position];
        isNegative = ch == '-';
        if (ch == '-' || ch == '+') {
          position += 1;
        }
      }
      int64_t result = parseNumber() * scale;
      while (position < length && scale > 1 && ruleString[position] == ':') {
        scale /= 60;
        position += 1;
        result += parseNumber() * scale;
      }
      if (isNegative) {
        result = -result;
      }
      return result;
    }

    /**
     * Parse a transition of the following form:
     *   ,(J<number>|<number>|M<number>.<number>.<number>)(/<offset>)?
     */
    void parseTransition(Transition& transition) {
      if (length - position < 2 || ruleString[position] != ',') {
        throwError("missing transition");
      }
      position += 1;
      char ch = ruleString[position];
      if (ch == 'J') {
        transition.kind = TRANSITION_JULIAN;
        position += 1;
        transition.day = parseNumber();
      } else if (ch == 'M') {
        transition.kind = TRANSITION_MONTH;
        position += 1;
        transition.month = parseNumber();
        if (position == length || ruleString[position] != '.') {
          throwError("missing first .");
        }
        position += 1;
        transition.week = parseNumber();
        if (position == length || ruleString[position] != '.') {
          throwError("missing second .");
        }
        position += 1;
        transition.day = parseNumber();
      } else {
        transition.kind = TRANSITION_DAY;
        transition.day = parseNumber();
      }
      if (position < length && ruleString[position] == '/') {
        position += 1;
        transition.time = parseOffset();
      } else {
        transition.time = 2 * 60 * 60;
      }
    }
  };

  /**
   * Parse the POSIX TZ string.
   */
  std::shared_ptr<FutureRule> parseFutureRule(const std::string& ruleString) {
    auto result = std::make_shared<FutureRuleImpl>();
    FutureRuleParser parser(ruleString, dynamic_cast<FutureRuleImpl*>(result.get()));
    return std::move(result);
  }

  std::string TimezoneVariant::toString() const {
    std::stringstream buffer;
    buffer << name << " " << gmtOffset;
    if (isDst) {
      buffer << " (dst)";
    }
    return buffer.str();
  }

  /**
   * An abstraction of the differences between versions.
   */
  class VersionParser {
   public:
    virtual ~VersionParser();

    /**
     * Get the version number.
     */
    virtual uint64_t getVersion() const = 0;

    /**
     * Get the number of bytes
     */
    virtual uint64_t getTimeSize() const = 0;

    /**
     * Parse the time at the given location.
     */
    virtual int64_t parseTime(const unsigned char* ptr) const = 0;

    /**
     * Parse the future string
     */
    virtual std::string parseFutureString(const unsigned char* ptr, uint64_t offset,
                                          uint64_t length) const = 0;
  };

  VersionParser::~VersionParser() {
    // PASS
  }

  static uint32_t decode32(const unsigned char* ptr) {
    return static_cast<uint32_t>(ptr[0] << 24) | static_cast<uint32_t>(ptr[1] << 16) |
           static_cast<uint32_t>(ptr[2] << 8) | static_cast<uint32_t>(ptr[3]);
  }

  class Version1Parser : public VersionParser {
   public:
    virtual ~Version1Parser() override;

    virtual uint64_t getVersion() const override {
      return 1;
    }

    /**
     * Get the number of bytes
     */
    virtual uint64_t getTimeSize() const override {
      return 4;
    }

    /**
     * Parse the time at the given location.
     */
    virtual int64_t parseTime(const unsigned char* ptr) const override {
      // sign extend from 32 bits
      return static_cast<int32_t>(decode32(ptr));
    }

    virtual std::string parseFutureString(const unsigned char*, uint64_t, uint64_t) const override {
      return "";
    }
  };

  Version1Parser::~Version1Parser() {
    // PASS
  }

  class Version2Parser : public VersionParser {
   public:
    virtual ~Version2Parser() override;

    virtual uint64_t getVersion() const override {
      return 2;
    }

    /**
     * Get the number of bytes
     */
    virtual uint64_t getTimeSize() const override {
      return 8;
    }

    /**
     * Parse the time at the given location.
     */
    virtual int64_t parseTime(const unsigned char* ptr) const override {
      return static_cast<int64_t>(decode32(ptr)) << 32 | decode32(ptr + 4);
    }

    virtual std::string parseFutureString(const unsigned char* ptr, uint64_t offset,
                                          uint64_t length) const override {
      return std::string(reinterpret_cast<const char*>(ptr) + offset + 1, length - 2);
    }
  };

  Version2Parser::~Version2Parser() {
    // PASS
  }

  class TimezoneImpl : public Timezone {
   public:
    TimezoneImpl(const std::string& _filename, const std::vector<unsigned char>& buffer);
    virtual ~TimezoneImpl() override;

    /**
     * Get the variant for the given time (time_t).
     */
    const TimezoneVariant& getVariant(int64_t clk) const override;

    void print(std::ostream&) const override;

    uint64_t getVersion() const override {
      return version;
    }

    int64_t getEpoch() const override {
      return epoch;
    }

    int64_t convertToUTC(int64_t clk) const override {
      return clk + getVariant(clk).gmtOffset;
    }

   private:
    void parseTimeVariants(const unsigned char* ptr, uint64_t variantOffset, uint64_t variantCount,
                           uint64_t nameOffset, uint64_t nameCount);
    void parseZoneFile(const unsigned char* ptr, uint64_t sectionOffset, uint64_t fileLength,
                       const VersionParser& version);
    // filename
    std::string filename;

    // the version of the file
    uint64_t version;

    // the list of variants for this timezone
    std::vector<TimezoneVariant> variants;

    // the list of the times where the local rules change
    std::vector<int64_t> transitions;

    // the variant that starts at this transition.
    std::vector<uint64_t> currentVariant;

    // the variant before the first transition
    uint64_t ancientVariant;

    // the rule for future times
    std::shared_ptr<FutureRule> futureRule;

    // the last explicit transition after which we use the future rule
    int64_t lastTransition;

    // The ORC epoch time in this timezone.
    int64_t epoch;
  };

  DIAGNOSTIC_PUSH
#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Wglobal-constructors")
  DIAGNOSTIC_IGNORE("-Wexit-time-destructors")
#endif
  static std::mutex timezone_mutex;
  static std::map<std::string, std::shared_ptr<Timezone> > timezoneCache;
  DIAGNOSTIC_POP

  Timezone::~Timezone() {
    // PASS
  }

  TimezoneImpl::TimezoneImpl(const std::string& _filename, const std::vector<unsigned char>& buffer)
      : filename(_filename) {
    parseZoneFile(&buffer[0], 0, buffer.size(), Version1Parser());
    // Build the literal for the ORC epoch
    // 2015 Jan 1 00:00:00
    tm epochStruct;
    epochStruct.tm_sec = 0;
    epochStruct.tm_min = 0;
    epochStruct.tm_hour = 0;
    epochStruct.tm_mday = 1;
    epochStruct.tm_mon = 0;
    epochStruct.tm_year = 2015 - 1900;
    epochStruct.tm_isdst = 0;
    time_t utcEpoch = timegm(&epochStruct);
    epoch = utcEpoch - getVariant(utcEpoch).gmtOffset;
  }

  const char* getTimezoneDirectory() {
    const char* dir = getenv("TZDIR");
    if (!dir) {
      dir = DEFAULT_TZDIR;
    }
    return dir;
  }

  /**
   * Get a timezone by absolute filename.
   * Results are cached.
   */
  const Timezone& getTimezoneByFilename(const std::string& filename) {
    // ORC-110
    std::lock_guard<std::mutex> timezone_lock(timezone_mutex);
    std::map<std::string, std::shared_ptr<Timezone> >::iterator itr = timezoneCache.find(filename);
    if (itr != timezoneCache.end()) {
      return *(itr->second).get();
    }
    try {
      std::unique_ptr<InputStream> file = readFile(filename);
      size_t size = static_cast<size_t>(file->getLength());
      std::vector<unsigned char> buffer(size);
      file->read(&buffer[0], size, 0);
      timezoneCache[filename] = std::make_shared<TimezoneImpl>(filename, buffer);
    } catch (ParseError& err) {
      throw TimezoneError(err.what());
    }
    return *timezoneCache[filename].get();
  }

  /**
   * Get the local timezone.
   */
  const Timezone& getLocalTimezone() {
#ifdef _MSC_VER
    return getTimezoneByName("UTC");
#else
    return getTimezoneByFilename(LOCAL_TIMEZONE);
#endif
  }

  /**
   * Get a timezone by name (eg. America/Los_Angeles).
   * Results are cached.
   */
  const Timezone& getTimezoneByName(const std::string& zone) {
    std::string filename(getTimezoneDirectory());
    filename += "/";
    filename += zone;
    return getTimezoneByFilename(filename);
  }

  /**
   * Parse a set of bytes as a timezone file as if they came from filename.
   */
  std::unique_ptr<Timezone> getTimezone(const std::string& filename,
                                        const std::vector<unsigned char>& b) {
    return std::make_unique<TimezoneImpl>(filename, b);
  }

  TimezoneImpl::~TimezoneImpl() {
    // PASS
  }

  void TimezoneImpl::parseTimeVariants(const unsigned char* ptr, uint64_t variantOffset,
                                       uint64_t variantCount, uint64_t nameOffset,
                                       uint64_t nameCount) {
    for (uint64_t variant = 0; variant < variantCount; ++variant) {
      variants[variant].gmtOffset =
          static_cast<int32_t>(decode32(ptr + variantOffset + 6 * variant));
      variants[variant].isDst = ptr[variantOffset + 6 * variant + 4] != 0;
      uint64_t nameStart = ptr[variantOffset + 6 * variant + 5];
      if (nameStart >= nameCount) {
        std::stringstream buffer;
        buffer << "name out of range in variant " << variant << " - " << nameStart
               << " >= " << nameCount;
        throw TimezoneError(buffer.str());
      }
      variants[variant].name =
          std::string(reinterpret_cast<const char*>(ptr) + nameOffset + nameStart);
    }
  }

  /**
   * Parse the zone file to get the bits we need.
   * There are two versions of the timezone file:
   *
   * Version 1(version = 0x00):
   *   Magic(version)
   *   Header
   *   TransitionTimes(4 byte)
   *   TransitionRules
   *   Rules
   *   LeapSeconds(4 byte)
   *   IsStd
   *   IsGmt
   *
   * Version2:
   *   Version1(0x32) = a version 1 copy of the data for old clients
   *   Magic(0x32)
   *   Header
   *   TransitionTimes(8 byte)
   *   TransitionRules
   *   Rules
   *   LeapSeconds(8 byte)
   *   IsStd
   *   IsGmt
   *   FutureString
   */
  void TimezoneImpl::parseZoneFile(const unsigned char* ptr, uint64_t sectionOffset,
                                   uint64_t fileLength, const VersionParser& versionParser) {
    const uint64_t magicOffset = sectionOffset + 0;
    const uint64_t headerOffset = magicOffset + 20;

    // check for validity before we start parsing
    if (fileLength < headerOffset + 6 * 4 ||
        strncmp(reinterpret_cast<const char*>(ptr) + magicOffset, "TZif", 4) != 0) {
      std::stringstream buffer;
      buffer << "non-tzfile " << filename;
      throw TimezoneError(buffer.str());
    }

    const uint64_t isGmtCount = decode32(ptr + headerOffset + 0);
    const uint64_t isStdCount = decode32(ptr + headerOffset + 4);
    const uint64_t leapCount = decode32(ptr + headerOffset + 8);
    const uint64_t timeCount = decode32(ptr + headerOffset + 12);
    const uint64_t variantCount = decode32(ptr + headerOffset + 16);
    const uint64_t nameCount = decode32(ptr + headerOffset + 20);

    const uint64_t timeOffset = headerOffset + 24;
    const uint64_t timeVariantOffset = timeOffset + versionParser.getTimeSize() * timeCount;
    const uint64_t variantOffset = timeVariantOffset + timeCount;
    const uint64_t nameOffset = variantOffset + variantCount * 6;
    const uint64_t sectionLength = nameOffset + nameCount +
                                   (versionParser.getTimeSize() + 4) * leapCount + isGmtCount +
                                   isStdCount;

    if (sectionLength > fileLength) {
      std::stringstream buffer;
      buffer << "tzfile too short " << filename << " needs " << sectionLength << " and has "
             << fileLength;
      throw TimezoneError(buffer.str());
    }

    // if it is version 2, skip over the old layout and read the new one.
    if (sectionOffset == 0 && ptr[magicOffset + 4] != 0) {
      parseZoneFile(ptr, sectionLength, fileLength, Version2Parser());
      return;
    }
    version = versionParser.getVersion();
    variants.resize(variantCount);
    transitions.resize(timeCount);
    currentVariant.resize(timeCount);
    parseTimeVariants(ptr, variantOffset, variantCount, nameOffset, nameCount);
    bool foundAncient = false;
    for (uint64_t t = 0; t < timeCount; ++t) {
      transitions[t] = versionParser.parseTime(ptr + timeOffset + t * versionParser.getTimeSize());
      currentVariant[t] = ptr[timeVariantOffset + t];
      if (currentVariant[t] >= variantCount) {
        std::stringstream buffer;
        buffer << "tzfile rule out of range " << filename << " references rule "
               << currentVariant[t] << " of " << variantCount;
        throw TimezoneError(buffer.str());
      }
      // find the oldest standard time and use that as the ancient value
      if (!foundAncient && !variants[currentVariant[t]].isDst) {
        foundAncient = true;
        ancientVariant = currentVariant[t];
      }
    }
    if (!foundAncient) {
      ancientVariant = 0;
    }
    futureRule = parseFutureRule(
        versionParser.parseFutureString(ptr, sectionLength, fileLength - sectionLength));

    // find the lower bound for applying the future rule
    if (futureRule->isDefined()) {
      if (timeCount > 0) {
        lastTransition = transitions[timeCount - 1];
      } else {
        lastTransition = INT64_MIN;
      }
    } else {
      lastTransition = INT64_MAX;
    }
  }

  const TimezoneVariant& TimezoneImpl::getVariant(int64_t clk) const {
    // if it is after the last explicit entry in the table,
    // use the future rule to get an answer
    if (clk > lastTransition) {
      return futureRule->getVariant(clk);
    } else {
      int64_t transition = binarySearch(transitions, clk);
      uint64_t idx;
      if (transition < 0) {
        idx = ancientVariant;
      } else {
        idx = currentVariant[static_cast<size_t>(transition)];
      }
      return variants[idx];
    }
  }

  void TimezoneImpl::print(std::ostream& out) const {
    out << "Timezone file: " << filename << "\n";
    out << "  Version: " << version << "\n";
    futureRule->print(out);
    for (uint64_t r = 0; r < variants.size(); ++r) {
      out << "  Variant " << r << ": " << variants[r].toString() << "\n";
    }
    for (uint64_t t = 0; t < transitions.size(); ++t) {
      tm timeStruct;
      tm* result = nullptr;
      char buffer[25];
      if (sizeof(time_t) >= 8) {
        time_t val = transitions[t];
        result = gmtime_r(&val, &timeStruct);
        if (result) {
          strftime(buffer, sizeof(buffer), "%F %H:%M:%S", &timeStruct);
        }
      }
      std::cout << "  Transition: " << (result == nullptr ? "null" : buffer) << " ("
                << transitions[t] << ") -> " << variants[currentVariant[t]].name << "\n";
    }
  }

  TimezoneError::TimezoneError(const std::string& what) : std::runtime_error(what) {
    // PASS
  }

  TimezoneError::TimezoneError(const TimezoneError& other) : std::runtime_error(other) {
    // PASS
  }

  TimezoneError::~TimezoneError() noexcept {
    // PASS
  }

}  // namespace orc
