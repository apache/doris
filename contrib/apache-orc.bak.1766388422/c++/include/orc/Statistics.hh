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

#ifndef ORC_STATISTICS_HH
#define ORC_STATISTICS_HH

#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/orc-config.hh"

#include <sstream>

namespace orc {

  /**
   * Statistics that are available for all types of columns.
   */
  class ColumnStatistics {
   public:
    virtual ~ColumnStatistics();

    /**
     * Get the number of values in this column. It will differ from the number
     * of rows because of NULL values.
     * @return the number of values
     */
    virtual uint64_t getNumberOfValues() const = 0;

    /**
     * Check whether column has null value.
     * @return true if has null value
     */
    virtual bool hasNull() const = 0;

    /**
     * Print out statistics of column if any.
     */
    virtual std::string toString() const = 0;
  };

  /**
   * Statistics for binary columns.
   */
  class BinaryColumnStatistics : public ColumnStatistics {
   public:
    ~BinaryColumnStatistics() override;

    /**
     * Check whether column has total length.
     * @return true if has total length
     */
    virtual bool hasTotalLength() const = 0;

    virtual uint64_t getTotalLength() const = 0;
  };

  /**
   * Statistics for boolean columns.
   */
  class BooleanColumnStatistics : public ColumnStatistics {
   public:
    ~BooleanColumnStatistics() override;

    /**
     * Check whether column has true/false count.
     * @return true if has true/false count
     */
    virtual bool hasCount() const = 0;

    virtual uint64_t getFalseCount() const = 0;
    virtual uint64_t getTrueCount() const = 0;
  };

  /**
   * Statistics for date columns.
   */
  class DateColumnStatistics : public ColumnStatistics {
   public:
    ~DateColumnStatistics() override;

    /**
     * Check whether column has minimum.
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * Check whether column has maximum.
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual int32_t getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int32_t getMaximum() const = 0;
  };

  /**
   * Statistics for decimal columns.
   */
  class DecimalColumnStatistics : public ColumnStatistics {
   public:
    ~DecimalColumnStatistics() override;

    /**
     * Check whether column has minimum.
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * Check whether column has maximum.
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Check whether column has sum.
     * @return true if has sum
     */
    virtual bool hasSum() const = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual Decimal getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual Decimal getMaximum() const = 0;

    /**
     * Get the sum for the column.
     * @return sum of all the values
     */
    virtual Decimal getSum() const = 0;
  };

  /**
   * Statistics for float and double columns.
   */
  class DoubleColumnStatistics : public ColumnStatistics {
   public:
    ~DoubleColumnStatistics() override;

    /**
     * Check whether column has minimum.
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * Check whether column has maximum.
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Check whether column has sum.
     * @return true if has sum
     */
    virtual bool hasSum() const = 0;

    /**
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual double getMinimum() const = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual double getMaximum() const = 0;

    /**
     * Get the sum of the values in the column.
     * @return the sum
     */
    virtual double getSum() const = 0;
  };

  /**
   * Statistics for all of the integer columns, such as byte, short, int, and
   * long.
   */
  class IntegerColumnStatistics : public ColumnStatistics {
   public:
    ~IntegerColumnStatistics() override;

    /**
     * Check whether column has minimum.
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * Check whether column has maximum.
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Check whether column has sum.
     * @return true if has sum
     */
    virtual bool hasSum() const = 0;

    /**
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * Get the sum of the column. Only valid if isSumDefined returns true.
     * @return the sum of the column
     */
    virtual int64_t getSum() const = 0;
  };

  /**
   * Statistics for string columns.
   */
  class StringColumnStatistics : public ColumnStatistics {
   public:
    ~StringColumnStatistics() override;

    /**
     * Check whether column has minimum.
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * Check whether column has maximum.
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Check whether column has total length.
     * @return true if has total length
     */
    virtual bool hasTotalLength() const = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual const std::string& getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual const std::string& getMaximum() const = 0;

    /**
     * Get the total length of all values.
     * @return total length of all the values
     */
    virtual uint64_t getTotalLength() const = 0;
  };

  /**
   * Statistics for timestamp columns.
   */
  class TimestampColumnStatistics : public ColumnStatistics {
   public:
    ~TimestampColumnStatistics() override;

    /**
     * Check whether minimum timestamp exists.
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * Check whether maximum timestamp exists.
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Get the millisecond of minimum timestamp in UTC.
     * @return minimum value in millisecond
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * Get the millisecond of maximum timestamp in UTC.
     * @return maximum value in millisecond
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * Check whether column has a lowerBound.
     * @return true if column has a lowerBound
     */
    virtual bool hasLowerBound() const = 0;

    /**
     * Check whether column has an upperBound.
     * @return true if column has an upperBound
     */
    virtual bool hasUpperBound() const = 0;

    /**
     * Get the lowerBound value for the column.
     * @return lowerBound value
     */
    virtual int64_t getLowerBound() const = 0;

    /**
     * Get the upperBound value for the column.
     * @return upperBound value
     */
    virtual int64_t getUpperBound() const = 0;

    /**
     * Get the last 6 digits of nanosecond of minimum timestamp.
     * @return last 6 digits of nanosecond of minimum timestamp.
     */
    virtual int32_t getMinimumNanos() const = 0;

    /**
     * Get the last 6 digits of nanosecond of maximum timestamp.
     * @return last 6 digits of nanosecond of maximum timestamp.
     */
    virtual int32_t getMaximumNanos() const = 0;
  };

  class Statistics {
   public:
    virtual ~Statistics();

    /**
     * Get the statistics of the given column.
     * @param colId id of the column
     * @return one column's statistics
     */
    virtual const ColumnStatistics* getColumnStatistics(uint32_t colId) const = 0;

    /**
     * Get the number of columns.
     * @return the number of columns
     */
    virtual uint32_t getNumberOfColumns() const = 0;
  };

  /**
   * Statistics for all of collections such as Map and List.
   */
  class CollectionColumnStatistics : public ColumnStatistics {
   public:
    ~CollectionColumnStatistics() override;

    /**
     * check whether column has minimum number of children
     * @return true if has minimum children count
     */
    virtual bool hasMinimumChildren() const = 0;

    /**
     * check whether column has maximum number of children
     * @return true if has maximum children count
     */
    virtual bool hasMaximumChildren() const = 0;

    /**
     * check whether column has total number of children
     * @return true if has total children count
     */
    virtual bool hasTotalChildren() const = 0;

    /**
     * set hasTotalChildren value
     * @param newHasTotalChildren hasTotalChildren value
     */
    virtual void setHasTotalChildren(bool newHasTotalChildren) = 0;

    /**
     * Get minimum number of children in the collection.
     * @return the minimum children count
     */
    virtual uint64_t getMinimumChildren() const = 0;

    /**
     * set new minimum children count
     * @param min new minimum children count
     */
    virtual void setMinimumChildren(uint64_t min) = 0;

    /**
     * Get maximum number of children in the collection.
     * @return the maximum children count
     */
    virtual uint64_t getMaximumChildren() const = 0;

    /**
     * set new maximum children count
     * @param max new maximum children count
     */
    virtual void setMaximumChildren(uint64_t max) = 0;

    /**
     * Get the total number of children in the collection.
     * @return the total number of children
     */
    virtual uint64_t getTotalChildren() const = 0;

    /**
     * set new total children count
     * @param newTotalChildrenCount total children count to be set
     */
    virtual void setTotalChildren(uint64_t newTotalChildrenCount) = 0;
  };

  class StripeStatistics : public Statistics {
   public:
    ~StripeStatistics() override;

    /**
     * Get the statistics of a given RowIndex entry in a given column.
     * @param columnId id of the column
     * @param rowIndexId RowIndex entry id
     * @return statistics of the given RowIndex entry
     */
    virtual const ColumnStatistics* getRowIndexStatistics(uint32_t columnId,
                                                          uint32_t rowIndexId) const = 0;

    /**
     * Get the number of RowIndex statistics in a given column.
     * @param columnId id of the column
     * @return the number of RowIndex statistics
     */
    virtual uint32_t getNumberOfRowIndexStats(uint32_t columnId) const = 0;
  };
}  // namespace orc

#endif
