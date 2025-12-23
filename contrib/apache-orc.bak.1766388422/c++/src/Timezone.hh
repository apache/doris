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

#ifndef TIMEZONE_HH
#define TIMEZONE_HH

// This file is for timezone routines.

#include "Adaptor.hh"

#include <stdint.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace orc {

  static const int64_t SECONDS_PER_HOUR = 60 * 60;
  static const int64_t SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

  /**
   * A variant  (eg. PST or PDT) of a timezone (eg. America/Los_Angeles).
   */
  struct TimezoneVariant {
    int64_t gmtOffset;
    bool isDst;
    std::string name;

    bool hasSameTzRule(const TimezoneVariant& other) const {
      return gmtOffset == other.gmtOffset && isDst == other.isDst;
    }

    std::string toString() const;
  };

  /**
   * A region that shares the same legal rules for wall clock time and
   * day light savings transitions. They are typically named for the largest
   * city in the region (eg. America/Los_Angeles or America/Mexico_City).
   */
  class Timezone {
   public:
    virtual ~Timezone();

    /**
     * Get the variant for the given time (time_t).
     */
    virtual const TimezoneVariant& getVariant(int64_t clk) const = 0;

    /**
     * Get the number of seconds between the ORC epoch in this timezone
     * and Unix epoch.
     * ORC epoch is 1 Jan 2015 00:00:00 local.
     * Unix epoch is 1 Jan 1970 00:00:00 UTC.
     */
    virtual int64_t getEpoch() const = 0;

    /**
     * Print the timezone to the stream.
     */
    virtual void print(std::ostream&) const = 0;

    /**
     * Get the version of the zone file.
     */
    virtual uint64_t getVersion() const = 0;

    /**
     * Convert wall clock time of current timezone to UTC timezone
     */
    virtual int64_t convertToUTC(int64_t clk) const = 0;
  };

  /**
   * Get the local timezone.
   * Results are cached.
   */
  const Timezone& getLocalTimezone();

  /**
   * Get a timezone by name (eg. America/Los_Angeles).
   * Results are cached.
   */
  const Timezone& getTimezoneByName(const std::string& zone);

  /**
   * Parse a set of bytes as a timezone file as if they came from filename.
   */
  std::unique_ptr<Timezone> getTimezone(const std::string& filename,
                                        const std::vector<unsigned char>& b);

  class TimezoneError : public std::runtime_error {
   public:
    explicit TimezoneError(const std::string& what);
    explicit TimezoneError(const TimezoneError&);
    ~TimezoneError() noexcept override;
  };

  /**
   * Represents the parsed POSIX timezone rule strings that are used to
   * describe the future transitions, because they can go arbitrarily far into
   * the future.
   */
  class FutureRule {
   public:
    virtual ~FutureRule();
    virtual bool isDefined() const = 0;
    virtual const TimezoneVariant& getVariant(int64_t clk) const = 0;
    virtual void print(std::ostream& out) const = 0;
  };

  /**
   * Parse the POSIX TZ string.
   */
  std::shared_ptr<FutureRule> parseFutureRule(const std::string& ruleString);
}  // namespace orc

#endif
