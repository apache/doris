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
package org.threeten.extra.chrono;

import java.io.Serializable;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoLocalDateTime;
import java.time.chrono.ChronoPeriod;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.time.temporal.TemporalUnit;
import java.time.temporal.ValueRange;
import java.util.Objects;

import static org.threeten.extra.chrono.HybridChronology.CUTOVER;
import static org.threeten.extra.chrono.HybridChronology.CUTOVER_DAYS;
import static org.threeten.extra.chrono.HybridChronology.CUTOVER_YEAR;

/**
 * A date in the British Cutover calendar system.
 * <p>
 * This date operates using the {@linkplain HybridChronology British Cutover calendar}.
 *
 * <h3>Implementation Requirements</h3>
 * This class is immutable and thread-safe.
 * <p>
 * This class must be treated as a value type. Do not synchronize, rely on the
 * identity hash code or use the distinction between equals() and ==.
 */
public final class HybridDate
    extends AbstractDate
    implements ChronoLocalDate, Serializable {
  /**
   * Serialization version.
   */
  private static final long serialVersionUID = -9626278512674L;
  /**
   * The underlying date.
   */
  private final LocalDate isoDate;
  /**
   * The underlying Julian date if before the cutover.
   */
  private final transient JulianDate julianDate;

  //-----------------------------------------------------------------------

  /**
   * Obtains the current {@code HybridDate} from the system clock in the default time-zone.
   * <p>
   * This will query the {@link Clock#systemDefaultZone() system clock} in the default
   * time-zone to obtain the current date.
   * <p>
   * Using this method will prevent the ability to use an alternate clock for testing
   * because the clock is hard-coded.
   *
   * @return the current date using the system clock and default time-zone, not null
   */
  public static HybridDate now() {
    return now(Clock.systemDefaultZone());
  }

  /**
   * Obtains the current {@code HybridDate} from the system clock in the specified time-zone.
   * <p>
   * This will query the {@link Clock#system(ZoneId) system clock} to obtain the current date.
   * Specifying the time-zone avoids dependence on the default time-zone.
   * <p>
   * Using this method will prevent the ability to use an alternate clock for testing
   * because the clock is hard-coded.
   *
   * @param zone the zone ID to use, not null
   * @return the current date using the system clock, not null
   */
  public static HybridDate now(ZoneId zone) {
    return now(Clock.system(zone));
  }

  /**
   * Obtains the current {@code HybridDate} from the specified clock.
   * <p>
   * This will query the specified clock to obtain the current date - today.
   * Using this method allows the use of an alternate clock for testing.
   * The alternate clock may be introduced using {@linkplain Clock dependency injection}.
   *
   * @param clock the clock to use, not null
   * @return the current date, not null
   * @throws DateTimeException if the current date cannot be obtained
   */
  public static HybridDate now(Clock clock) {
    return new HybridDate(LocalDate.now(clock));
  }

  /**
   * Obtains a {@code HybridDate} representing a date in the British Cutover calendar
   * system from the proleptic-year, month-of-year and day-of-month fields.
   * <p>
   * This returns a {@code HybridDate} with the specified fields.
   * <p>
   * Dates in the middle of the cutover gap, such as the 10th September 1752,
   * will not throw an exception. Instead, the date will be treated as a Julian date
   * and converted to an ISO date, with the day of month shifted by 11 days.
   * <p>
   * Invalid dates, such as September 31st will throw an exception.
   *
   * @param prolepticYear the British Cutover proleptic-year
   * @param month         the British Cutover month-of-year, from 1 to 12
   * @param dayOfMonth    the British Cutover day-of-month, from 1 to 31
   * @return the date in British Cutover calendar system, not null
   * @throws DateTimeException if the value of any field is out of range,
   *                           or if the day-of-month is invalid for the month-year
   */
  public static HybridDate of(int prolepticYear, int month, int dayOfMonth) {
    return HybridDate.create(prolepticYear, month, dayOfMonth);
  }

  /**
   * Obtains a {@code HybridDate} from a temporal object.
   * <p>
   * This obtains a date in the British Cutover calendar system based on the specified temporal.
   * A {@code TemporalAccessor} represents an arbitrary set of date and time information,
   * which this factory converts to an instance of {@code HybridDate}.
   * <p>
   * The conversion uses the {@link ChronoField#EPOCH_DAY EPOCH_DAY}
   * field, which is standardized across calendar systems.
   * <p>
   * This method matches the signature of the functional interface {@link TemporalQuery}
   * allowing it to be used as a query via method reference, {@code HybridDate::from}.
   *
   * @param temporal the temporal object to convert, not null
   * @return the date in British Cutover calendar system, not null
   * @throws DateTimeException if unable to convert to a {@code HybridDate}
   */
  public static HybridDate from(TemporalAccessor temporal) {
    if (temporal instanceof HybridDate) {
      return (HybridDate) temporal;
    }
    return new HybridDate(LocalDate.from(temporal));
  }

  //-----------------------------------------------------------------------

  /**
   * Obtains a {@code HybridDate} representing a date in the British Cutover calendar
   * system from the proleptic-year and day-of-year fields.
   * <p>
   * This returns a {@code HybridDate} with the specified fields.
   * The day must be valid for the year, otherwise an exception will be thrown.
   *
   * @param prolepticYear the British Cutover proleptic-year
   * @param dayOfYear     the British Cutover day-of-year, from 1 to 366
   * @return the date in British Cutover calendar system, not null
   * @throws DateTimeException if the value of any field is out of range,
   *                           or if the day-of-year is invalid for the year
   */
  static HybridDate ofYearDay(int prolepticYear, int dayOfYear) {
    if (prolepticYear < CUTOVER_YEAR || (prolepticYear == CUTOVER_YEAR && dayOfYear <= 246)) {
      JulianDate julian = JulianDate.ofYearDay(prolepticYear, dayOfYear);
      return new HybridDate(julian);
    } else if (prolepticYear == CUTOVER_YEAR) {
      LocalDate iso = LocalDate.ofYearDay(prolepticYear, dayOfYear + CUTOVER_DAYS);
      return new HybridDate(iso);
    } else {
      LocalDate iso = LocalDate.ofYearDay(prolepticYear, dayOfYear);
      return new HybridDate(iso);
    }
  }

  /**
   * Obtains a {@code HybridDate} representing a date in the British Cutover calendar
   * system from the epoch-day.
   *
   * @param epochDay the epoch day to convert based on 1970-01-01 (ISO)
   * @return the date in British Cutover calendar system, not null
   * @throws DateTimeException if the epoch-day is out of range
   */
  static HybridDate ofEpochDay(final long epochDay) {
    return new HybridDate(LocalDate.ofEpochDay(epochDay));
  }

  /**
   * Creates a {@code HybridDate} validating the input.
   *
   * @param prolepticYear the British Cutover proleptic-year
   * @param month         the British Cutover month-of-year, from 1 to 12
   * @param dayOfMonth    the British Cutover day-of-month, from 1 to 31
   * @return the date in British Cutover calendar system, not null
   * @throws DateTimeException if the value of any field is out of range,
   *                           or if the day-of-month is invalid for the month-year
   */
  static HybridDate create(int prolepticYear, int month, int dayOfMonth) {
    if (prolepticYear < CUTOVER_YEAR) {
      JulianDate julian = JulianDate.of(prolepticYear, month, dayOfMonth);
      return new HybridDate(julian);
    } else {
      LocalDate iso = LocalDate.of(prolepticYear, month, dayOfMonth);
      if (iso.isBefore(CUTOVER)) {
        JulianDate julian = JulianDate.of(prolepticYear, month, dayOfMonth);
        return new HybridDate(julian);
      }
      return new HybridDate(iso);
    }
  }

  //-----------------------------------------------------------------------

  /**
   * Creates an instance from an ISO date.
   *
   * @param isoDate the standard local date, not null
   */
  HybridDate(LocalDate isoDate) {
    Objects.requireNonNull(isoDate, "isoDate");
    this.isoDate = isoDate;
    this.julianDate = (isoDate.isBefore(CUTOVER) ? JulianDate.from(isoDate) : null);
  }

  /**
   * Creates an instance from a Julian date.
   *
   * @param julianDate the Julian date before the cutover, not null
   */
  HybridDate(JulianDate julianDate) {
    Objects.requireNonNull(julianDate, "julianDate");
    this.isoDate = LocalDate.from(julianDate);
    this.julianDate = (isoDate.isBefore(CUTOVER) ? julianDate : null);
  }

  /**
   * Validates the object.
   *
   * @return the resolved date, not null
   */
  private Object readResolve() {
    return new HybridDate(isoDate);
  }

  //-----------------------------------------------------------------------
  private boolean isCutoverYear() {
    return isoDate.getYear() == CUTOVER_YEAR && isoDate.getDayOfYear() > CUTOVER_DAYS;
  }

  private boolean isCutoverMonth() {
    return isoDate.getYear() == CUTOVER_YEAR &&
        isoDate.getMonthValue() == 9 && isoDate.getDayOfMonth() > CUTOVER_DAYS;
  }

  //-------------------------------------------------------------------------
  @Override
  int getAlignedDayOfWeekInMonth() {
    if (isCutoverMonth() && julianDate == null) {
      return ((getDayOfMonth() - 1 - CUTOVER_DAYS) % lengthOfWeek()) + 1;
    }
    return super.getAlignedDayOfWeekInMonth();
  }

  @Override
  int getAlignedWeekOfMonth() {
    if (isCutoverMonth() && julianDate == null) {
      return ((getDayOfMonth() - 1 - CUTOVER_DAYS) / lengthOfWeek()) + 1;
    }
    return super.getAlignedWeekOfMonth();
  }

  @Override
  int getProlepticYear() {
    return (julianDate != null ? julianDate.getProlepticYear() : isoDate.getYear());
  }

  @Override
  int getMonth() {
    return (julianDate != null ? julianDate.getMonth() : isoDate.getMonthValue());
  }

  @Override
  int getDayOfMonth() {
    return (julianDate != null ? julianDate.getDayOfMonth() : isoDate.getDayOfMonth());
  }

  @Override
  int getDayOfYear() {
    if (julianDate != null) {
      return julianDate.getDayOfYear();
    }
    if (isoDate.getYear() == CUTOVER_YEAR) {
      return isoDate.getDayOfYear() - CUTOVER_DAYS;
    }
    return isoDate.getDayOfYear();
  }

  @Override
  public ValueRange rangeChrono(ChronoField field) {
    switch (field) {
      case DAY_OF_MONTH:
        // short length, but value range still 1 to 30
        if (isCutoverMonth()) {
          return ValueRange.of(1, 30);
        }
        return ValueRange.of(1, lengthOfMonth());
      case DAY_OF_YEAR:
        // 1 to 355 in cutover year, otherwise 1 to 365/366
        return ValueRange.of(1, lengthOfYear());
      case ALIGNED_WEEK_OF_MONTH:
        // 1 to 3 in cutover month, otherwise 1 to 4/5
        return rangeAlignedWeekOfMonth();
      case ALIGNED_WEEK_OF_YEAR:
        // 1 to 51 in cutover year, otherwise 1 to 53
        if (isCutoverYear()) {
          return ValueRange.of(1, 51);
        }
        return ChronoField.ALIGNED_WEEK_OF_YEAR.range();
      default:
        return getChronology().range(field);
    }
  }

  @Override
  ValueRange rangeAlignedWeekOfMonth() {
    if (isCutoverMonth()) {
      return ValueRange.of(1, 3);
    }
    return ValueRange.of(1, getMonth() == 2 && isLeapYear() == false ? 4 : 5);
  }

  @Override
  HybridDate resolvePrevious(int year, int month, int dayOfMonth) {
    switch (month) {
      case 2:
        dayOfMonth = Math.min(dayOfMonth, getChronology().isLeapYear(year) ? 29 : 28);
        break;
      case 4:
      case 6:
      case 9:
      case 11:
        dayOfMonth = Math.min(dayOfMonth, 30);
        break;
      default:
        break;
    }
    return create(year, month, dayOfMonth);
  }

  //-----------------------------------------------------------------------

  /**
   * Gets the chronology of this date, which is the British Cutover calendar system.
   * <p>
   * The {@code Chronology} represents the calendar system in use.
   * The era and other fields in {@link ChronoField} are defined by the chronology.
   *
   * @return the British Cutover chronology, not null
   */
  @Override
  public HybridChronology getChronology() {
    return HybridChronology.INSTANCE;
  }

  /**
   * Gets the era applicable at this date.
   * <p>
   * The British Cutover calendar system has two eras, 'AD' and 'BC',
   * defined by {@link JulianEra}.
   *
   * @return the era applicable at this date, not null
   */
  @Override
  public JulianEra getEra() {
    return (getProlepticYear() >= 1 ? JulianEra.AD : JulianEra.BC);
  }

  /**
   * Returns the length of the month represented by this date.
   * <p>
   * This returns the length of the month in days.
   * This takes into account the cutover, returning 19 in September 1752.
   *
   * @return the length of the month in days, from 19 to 31
   */
  @Override
  public int lengthOfMonth() {
    if (isCutoverMonth()) {
      return 19;
    }
    return (julianDate != null ? julianDate.lengthOfMonth() : isoDate.lengthOfMonth());
  }

  /**
   * Returns the length of the year represented by this date.
   * <p>
   * This returns the length of the year in days.
   * This takes into account the cutover, returning 355 in 1752.
   *
   * @return the length of the month in days, from 19 to 31
   */
  @Override
  public int lengthOfYear() {
    if (isCutoverYear()) {
      return 355;
    }
    return (julianDate != null ? julianDate.lengthOfYear() : isoDate.lengthOfYear());
  }

  //-------------------------------------------------------------------------
  @Override
  public HybridDate with(TemporalAdjuster adjuster) {
    return (HybridDate) adjuster.adjustInto(this);
  }

  @Override
  public HybridDate with(TemporalField field, long newValue) {
    return (HybridDate) super.with(field, newValue);
  }

  //-----------------------------------------------------------------------
  @Override
  public HybridDate plus(TemporalAmount amount) {
    return (HybridDate) amount.addTo(this);
  }

  @Override
  public HybridDate plus(long amountToAdd, TemporalUnit unit) {
    return (HybridDate) super.plus(amountToAdd, unit);
  }

  @Override
  public HybridDate minus(TemporalAmount amount) {
    return (HybridDate) amount.subtractFrom(this);
  }

  @Override
  public HybridDate minus(long amountToSubtract, TemporalUnit unit) {
    return (amountToSubtract == Long.MIN_VALUE ?
        plus(Long.MAX_VALUE, unit).plus(1, unit) : plus(-amountToSubtract, unit));
  }

  //-------------------------------------------------------------------------
  @Override  // for covariant return type
  @SuppressWarnings("unchecked")
  public ChronoLocalDateTime<HybridDate> atTime(LocalTime localTime) {
    return (ChronoLocalDateTime<HybridDate>) super.atTime(localTime);
  }

  @Override
  public long until(Temporal endExclusive, TemporalUnit unit) {
    return super.until(HybridDate.from(endExclusive), unit);
  }

  @Override
  public ChronoPeriod until(ChronoLocalDate endDateExclusive) {
    HybridDate end = HybridDate.from(endDateExclusive);
    long totalMonths = end.getProlepticMonth() - this.getProlepticMonth();  // safe
    int days = end.getDayOfMonth() - this.getDayOfMonth();
    if (totalMonths == 0 && isCutoverMonth()) {
      if (julianDate != null && end.julianDate == null) {
        days -= CUTOVER_DAYS;
      } else if (julianDate == null && end.julianDate != null) {
        days += CUTOVER_DAYS;
      }
    } else if (totalMonths > 0) {
      if (julianDate != null && end.julianDate == null) {
        AbstractDate calcDate = this.plusMonths(totalMonths);
        days = (int) (end.toEpochDay() - calcDate.toEpochDay());  // safe
      }
      if (days < 0) {
        totalMonths--;
        AbstractDate calcDate = this.plusMonths(totalMonths);
        days = (int) (end.toEpochDay() - calcDate.toEpochDay());  // safe
      }
    } else if (totalMonths < 0 && days > 0) {
      totalMonths++;
      AbstractDate calcDate = this.plusMonths(totalMonths);
      days = (int) (end.toEpochDay() - calcDate.toEpochDay());  // safe
    }
    int years = Math.toIntExact(totalMonths / lengthOfYearInMonths());  // safe
    int months = (int) (totalMonths % lengthOfYearInMonths());  // safe
    return getChronology().period(years, months, days);
  }

  //-----------------------------------------------------------------------
  @Override
  public long toEpochDay() {
    return isoDate.toEpochDay();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> R query(TemporalQuery<R> query) {
    if (query == TemporalQueries.localDate()) {
      return (R) isoDate;
    }
    return super.query(query);
  }

  //-------------------------------------------------------------------------
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof HybridDate) {
      HybridDate otherDate = (HybridDate) obj;
      return this.isoDate.equals(otherDate.isoDate);
    }
    return false;
  }

  /**
   * A hash code for this date.
   *
   * @return a suitable hash code based only on the Chronology and the date
   */
  @Override
  public int hashCode() {
    return getChronology().getId().hashCode() ^ isoDate.hashCode();
  }

}
