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
package org.apache.orc.impl;

import org.threeten.extra.chrono.HybridChronology;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

/**
 * Conversion utilities from the hybrid Julian/Gregorian calendar to/from the
 * proleptic Gregorian.
 * <p>
 * The semantics here are to hold the string representation constant and change
 * the epoch offset rather than holding the instant in time constant and change
 * the string representation.
 * <p>
 * These utilities will be fast for the common case (&gt; 1582 AD), but slow
 * for old dates.
 */
public class DateUtils {
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneId LOCAL = ZoneId.systemDefault();
  private static final long SWITCHOVER_MILLIS;
  private static final long SWITCHOVER_DAYS;
  private static final DateTimeFormatter HYBRID_DATE_FORMAT =
      ConvertTreeReaderFactory.DATE_FORMAT
          .withChronology(HybridChronology.INSTANCE)
          .withZone(UTC);
  private static final DateTimeFormatter PROLEPTIC_DATE_FORMAT =
      DateTimeFormatter.ISO_LOCAL_DATE
          .withChronology(IsoChronology.INSTANCE)
          .withZone(UTC);
  private static final DateTimeFormatter HYBRID_UTC_TIME_FORMAT =
      ConvertTreeReaderFactory.TIMESTAMP_FORMAT
          .withChronology(HybridChronology.INSTANCE)
          .withZone(UTC);
  private static final DateTimeFormatter HYBRID_LOCAL_TIME_FORMAT =
      ConvertTreeReaderFactory.TIMESTAMP_FORMAT
          .withChronology(HybridChronology.INSTANCE)
          .withZone(LOCAL);
  private static final DateTimeFormatter PROLEPTIC_UTC_TIME_FORMAT =
      ConvertTreeReaderFactory.TIMESTAMP_FORMAT
          .withChronology(IsoChronology.INSTANCE)
          .withZone(UTC);
  private static final DateTimeFormatter PROLEPTIC_LOCAL_TIME_FORMAT =
      ConvertTreeReaderFactory.TIMESTAMP_FORMAT
          .withChronology(IsoChronology.INSTANCE)
          .withZone(LOCAL);

  static {
    // Get the last day where the two calendars agree with each other.
    SWITCHOVER_DAYS = LocalDate.from(HYBRID_DATE_FORMAT.parse("1582-10-15")).toEpochDay();
    SWITCHOVER_MILLIS = TimeUnit.DAYS.toMillis(SWITCHOVER_DAYS);
  }

  /**
   * Convert an epoch day from the hybrid Julian/Gregorian calendar to the
   * proleptic Gregorian.
   * @param hybrid day of epoch in the hybrid Julian/Gregorian
   * @return day of epoch in the proleptic Gregorian
   */
  public static int convertDateToProleptic(int hybrid) {
    int proleptic = hybrid;
    if (hybrid < SWITCHOVER_DAYS) {
      String dateStr = HYBRID_DATE_FORMAT.format(LocalDate.ofEpochDay(proleptic));
      proleptic = (int) LocalDate.from(PROLEPTIC_DATE_FORMAT.parse(dateStr)).toEpochDay();
    }
    return proleptic;
  }

  /**
   * Convert an epoch day from the proleptic Gregorian calendar to the hybrid
   * Julian/Gregorian.
   * @param proleptic day of epoch in the proleptic Gregorian
   * @return day of epoch in the hybrid Julian/Gregorian
   */
  public static int convertDateToHybrid(int proleptic) {
    int hybrid = proleptic;
    if (proleptic < SWITCHOVER_DAYS) {
      String dateStr = PROLEPTIC_DATE_FORMAT.format(LocalDate.ofEpochDay(proleptic));
      hybrid = (int) LocalDate.from(HYBRID_DATE_FORMAT.parse(dateStr)).toEpochDay();
    }
    return hybrid;
  }

  /**
   * Convert epoch millis from the hybrid Julian/Gregorian calendar to the
   * proleptic Gregorian.
   * @param hybrid millis of epoch in the hybrid Julian/Gregorian
   * @param useUtc use UTC instead of local
   * @return millis of epoch in the proleptic Gregorian
   */
  public static long convertTimeToProleptic(long hybrid, boolean useUtc) {
    long proleptic = hybrid;
    if (hybrid < SWITCHOVER_MILLIS) {
      if (useUtc) {
        String dateStr = HYBRID_UTC_TIME_FORMAT.format(Instant.ofEpochMilli(hybrid));
        proleptic = Instant.from(PROLEPTIC_UTC_TIME_FORMAT.parse(dateStr)).toEpochMilli();
      } else {
        String dateStr = HYBRID_LOCAL_TIME_FORMAT.format(Instant.ofEpochMilli(hybrid));
        proleptic = Instant.from(PROLEPTIC_LOCAL_TIME_FORMAT.parse(dateStr)).toEpochMilli();
      }
    }
    return proleptic;
  }

  /**
   * Convert epoch millis from the proleptic Gregorian calendar to the hybrid
   * Julian/Gregorian.
   * @param proleptic millis of epoch in the proleptic Gregorian
   * @param useUtc use UTC instead of local
   * @return millis of epoch in the hybrid Julian/Gregorian
   */
  public static long convertTimeToHybrid(long proleptic, boolean useUtc) {
    long hybrid = proleptic;
    if (proleptic < SWITCHOVER_MILLIS) {
      if (useUtc) {
        String dateStr = PROLEPTIC_UTC_TIME_FORMAT.format(Instant.ofEpochMilli(hybrid));
        hybrid = Instant.from(HYBRID_UTC_TIME_FORMAT.parse(dateStr)).toEpochMilli();
      } else {
        String dateStr = PROLEPTIC_LOCAL_TIME_FORMAT.format(Instant.ofEpochMilli(hybrid));
        hybrid = Instant.from(HYBRID_LOCAL_TIME_FORMAT.parse(dateStr)).toEpochMilli();
      }
    }
    return hybrid;
  }


  public static int convertDate(int original,
                                boolean fromProleptic,
                                boolean toProleptic) {
    if (fromProleptic != toProleptic) {
      return toProleptic
                 ? convertDateToProleptic(original)
                 : convertDateToHybrid(original);
    } else {
      return original;
    }
  }

  public static long convertTime(long original,
                                 boolean fromProleptic,
                                 boolean toProleptic,
                                 boolean useUtc) {
    if (fromProleptic != toProleptic) {
      return toProleptic
                 ? convertTimeToProleptic(original, useUtc)
                 : convertTimeToHybrid(original, useUtc);
    } else {
      return original;
    }
  }

  public static Integer parseDate(String date, boolean fromProleptic) {
    try {
      TemporalAccessor time =
          (fromProleptic ? PROLEPTIC_DATE_FORMAT : HYBRID_DATE_FORMAT).parse(date);
      return (int) LocalDate.from(time).toEpochDay();
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  public static String printDate(int date, boolean fromProleptic) {
    return (fromProleptic ? PROLEPTIC_DATE_FORMAT : HYBRID_DATE_FORMAT)
               .format(LocalDate.ofEpochDay(date));
  }

  public static DateTimeFormatter getTimeFormat(boolean useProleptic,
                                                boolean useUtc) {
    if (useProleptic) {
      return useUtc ? PROLEPTIC_UTC_TIME_FORMAT : PROLEPTIC_LOCAL_TIME_FORMAT;
    } else {
      return useUtc ? HYBRID_UTC_TIME_FORMAT : HYBRID_LOCAL_TIME_FORMAT;
    }
  }

  public static Long parseTime(String date, boolean fromProleptic, boolean useUtc) {
    try {
      TemporalAccessor time = getTimeFormat(fromProleptic, useUtc).parse(date);
      return Instant.from(time).toEpochMilli();
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  public static String printTime(long millis, boolean fromProleptic,
                                 boolean useUtc) {
    return getTimeFormat(fromProleptic, useUtc).format(Instant.ofEpochMilli(millis));
  }

  private DateUtils() {
    throw new UnsupportedOperationException();
  }
}
