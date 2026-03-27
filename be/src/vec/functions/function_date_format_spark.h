// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either explicit or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <unicode/locid.h>
#include <unicode/stringpiece.h>
#include <unicode/timezone.h>
#include <unicode/unistr.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <iterator>
#include <ranges>
#include <span>
#include <string_view>
#include <vector>

#include "vec/functions/function_datetime_string_to_string.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

template <PrimitiveType PType>
struct DateFormatSparkImpl {
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;
    using ArgType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr PrimitiveType FromPType = PType;
    static constexpr auto name = "date_format_spark";

    static bool execute(const DateType& dt, StringRef format, ColumnString::Chars& res_data,
                        size_t& offset, const cctz::time_zone& time_zone) {
        std::string formatted;
        if (!format_date_time_spark(dt, format, time_zone, formatted)) {
            return true;
        }
        res_data.insert(formatted.data(), formatted.data() + formatted.size());
        offset += formatted.size();
        return false;
    }

private:
    enum class PatternKind {
        Literal,
        Era,
        Year,
        DayOfYear,
        MonthStandard,
        MonthStandalone,
        DayOfMonth,
        QuarterStandard,
        QuarterStandalone,
        DayOfWeekText,
        AlignedDayOfWeekInMonth,
        AmPm,
        ClockHourOfAmPm,
        HourOfAmPm,
        ClockHourOfDay,
        HourOfDay,
        Minute,
        Second,
        Fraction,
        ZoneId,
        ZoneName,
        LocalizedOffset,
        OffsetX,
        Offsetx,
        OffsetZ
    };

    struct PatternItem {
        PatternKind kind;
        int count;
        std::string literal;
    };

    struct ParsedPatternCache {
        std::string format;
        std::vector<PatternItem> items;
    };

    enum class PatternCategory { Literal, Date, Time, Zone };

    struct PatternRule {
        char letter;
        int min_count;
        int max_count;
        PatternKind kind;

        [[nodiscard]] constexpr bool matches(char c, int run_len) const noexcept {
            return c == letter && run_len >= min_count && run_len <= max_count;
        }
    };

    static constexpr std::array<std::string_view, 12> MONTH_NAMES_SHORT = {
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    static constexpr std::array<std::string_view, 12> MONTH_NAMES_LONG = {
            "January", "February", "March",     "April",   "May",      "June",
            "July",    "August",   "September", "October", "November", "December"};
    static constexpr std::array<std::string_view, 7> DAY_NAMES_SHORT = {"Mon", "Tue", "Wed", "Thu",
                                                                        "Fri", "Sat", "Sun"};
    static constexpr std::array<std::string_view, 7> DAY_NAMES_LONG = {
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    static constexpr std::array<std::string_view, 4> QUARTER_NAMES_SHORT = {"Q1", "Q2", "Q3", "Q4"};
    static constexpr std::array<std::string_view, 4> QUARTER_NAMES_LONG = {
            "1st quarter", "2nd quarter", "3rd quarter", "4th quarter"};
    static constexpr std::array<std::string_view, 2> ERA_NAMES_SHORT = {"BC", "AD"};
    static constexpr std::array<std::string_view, 2> ERA_NAMES_LONG = {"Before Christ",
                                                                       "Anno Domini"};
    static constexpr std::array<PatternRule, 7> DATE_PATTERN_RULES = {{
            {'G', 1, 4, PatternKind::Era},
            {'y', 1, 6, PatternKind::Year},
            {'u', 1, 6, PatternKind::Year},
            {'D', 1, 3, PatternKind::DayOfYear},
            {'M', 1, 4, PatternKind::MonthStandard},
            {'L', 1, 4, PatternKind::MonthStandalone},
            {'d', 1, 2, PatternKind::DayOfMonth},
    }};
    static constexpr std::array<PatternRule, 5> TEXT_PATTERN_RULES = {{
            {'Q', 1, 4, PatternKind::QuarterStandard},
            {'q', 1, 4, PatternKind::QuarterStandalone},
            {'E', 1, 4, PatternKind::DayOfWeekText},
            {'F', 1, 1, PatternKind::AlignedDayOfWeekInMonth},
            {'a', 1, 1, PatternKind::AmPm},
    }};
    static constexpr std::array<PatternRule, 7> TIME_PATTERN_RULES = {{
            {'h', 1, 2, PatternKind::ClockHourOfAmPm},
            {'K', 1, 2, PatternKind::HourOfAmPm},
            {'k', 1, 2, PatternKind::ClockHourOfDay},
            {'H', 1, 2, PatternKind::HourOfDay},
            {'m', 1, 2, PatternKind::Minute},
            {'s', 1, 2, PatternKind::Second},
            {'S', 1, 9, PatternKind::Fraction},
    }};
    static constexpr std::array<PatternRule, 7> ZONE_PATTERN_RULES = {{
            {'V', 2, 2, PatternKind::ZoneId},
            {'z', 1, 4, PatternKind::ZoneName},
            {'O', 1, 1, PatternKind::LocalizedOffset},
            {'O', 4, 4, PatternKind::LocalizedOffset},
            {'X', 1, 5, PatternKind::OffsetX},
            {'x', 1, 5, PatternKind::Offsetx},
            {'Z', 1, 5, PatternKind::OffsetZ},
    }};
    static constexpr std::array<std::span<const PatternRule>, 4> kPatternRuleTables {
            std::span<const PatternRule>(DATE_PATTERN_RULES),
            std::span<const PatternRule>(TEXT_PATTERN_RULES),
            std::span<const PatternRule>(TIME_PATTERN_RULES),
            std::span<const PatternRule>(ZONE_PATTERN_RULES),
    };

    static void append_number(std::string& out, int value, int min_width) {
        std::string str = std::to_string(value);
        if (value >= 0 && static_cast<int>(str.size()) < min_width) {
            out.append(min_width - str.size(), '0');
        }
        out.append(str);
    }

    static void append_year(std::string& out, int year, int count) {
        const int width = (count == 2) ? 2 : std::max(count, 1);
        append_number(out, width == 2 ? year % 100 : year, width);
    }

    static void append_month_value(std::string& out, int month, int count) {
        if (count <= 2) {
            append_number(out, month, count);
        } else if (count == 3) {
            out.append(MONTH_NAMES_SHORT[month - 1]);
        } else {
            out.append(MONTH_NAMES_LONG[month - 1]);
        }
    }

    static void append_quarter_value(std::string& out, int quarter, int count) {
        if (count <= 2) {
            append_number(out, quarter, count);
        } else if (count == 3) {
            out.append(QUARTER_NAMES_SHORT[quarter - 1]);
        } else {
            out.append(QUARTER_NAMES_LONG[quarter - 1]);
        }
    }

    static void append_weekday_value(std::string& out, int weekday, int count) {
        out.append(count == 4 ? DAY_NAMES_LONG[weekday] : DAY_NAMES_SHORT[weekday]);
    }

    static void append_fraction_value(std::string& out, int microsecond, int count) {
        std::string fraction = std::to_string(microsecond);
        if (fraction.size() < 6) {
            fraction.insert(fraction.begin(), 6 - fraction.size(), '0');
        }
        if (count <= 6) {
            out.append(fraction, 0, count);
        } else {
            out.append(fraction);
            out.append(count - 6, '0');
        }
    }

    static PatternCategory get_pattern_category(PatternKind kind) {
        switch (kind) {
        case PatternKind::Literal:
            return PatternCategory::Literal;
        case PatternKind::Era:
        case PatternKind::Year:
        case PatternKind::DayOfYear:
        case PatternKind::MonthStandard:
        case PatternKind::MonthStandalone:
        case PatternKind::DayOfMonth:
        case PatternKind::QuarterStandard:
        case PatternKind::QuarterStandalone:
        case PatternKind::DayOfWeekText:
        case PatternKind::AlignedDayOfWeekInMonth:
            return PatternCategory::Date;
        case PatternKind::AmPm:
        case PatternKind::ClockHourOfAmPm:
        case PatternKind::HourOfAmPm:
        case PatternKind::ClockHourOfDay:
        case PatternKind::HourOfDay:
        case PatternKind::Minute:
        case PatternKind::Second:
        case PatternKind::Fraction:
            return PatternCategory::Time;
        case PatternKind::ZoneId:
        case PatternKind::ZoneName:
        case PatternKind::LocalizedOffset:
        case PatternKind::OffsetX:
        case PatternKind::Offsetx:
        case PatternKind::OffsetZ:
            return PatternCategory::Zone;
        }
        DCHECK(false);
        return PatternCategory::Literal;
    }

    static cctz::time_zone::absolute_lookup lookup_timezone_info(const DateType& dt,
                                                                 const cctz::time_zone& time_zone) {
        int64_t ts = 0;
        dt.unix_timestamp(&ts, time_zone);
        auto tp = std::chrono::system_clock::from_time_t(ts);
        return time_zone.lookup(tp);
    }

    static bool append_timezone_display_name(const cctz::time_zone& time_zone,
                                             const cctz::time_zone::absolute_lookup& tz_info,
                                             bool long_name, std::string& out) {
        std::unique_ptr<icu::TimeZone> tz {icu::TimeZone::createTimeZone(
                icu::UnicodeString::fromUTF8(icu::StringPiece(time_zone.name())))};
        if (*tz == icu::TimeZone::getUnknown()) {
            return false;
        }
        icu::UnicodeString display;
        tz->getDisplayName(static_cast<UBool>(tz_info.is_dst),
                           long_name ? icu::TimeZone::LONG : icu::TimeZone::SHORT,
                           icu::Locale::getUS(), display);
        std::string utf8;
        display.toUTF8String(utf8);
        if (utf8.empty()) {
            return false;
        }
        out.append(utf8);
        return true;
    }

    static void append_numeric_zone_offset(std::string& out, int offset_seconds, bool with_colon,
                                           bool zero_as_z, bool append_seconds_if_nonzero) {
        if (offset_seconds == 0 && zero_as_z) {
            out.push_back('Z');
            return;
        }
        char sign = '+';
        if (offset_seconds < 0) {
            sign = '-';
            offset_seconds = -offset_seconds;
        }
        int hours = offset_seconds / 3600;
        int minutes = (offset_seconds % 3600) / 60;
        int seconds = offset_seconds % 60;
        out.push_back(sign);
        append_number(out, hours, 2);
        if (with_colon) {
            out.push_back(':');
        }
        append_number(out, minutes, 2);
        if (append_seconds_if_nonzero && seconds != 0) {
            if (with_colon) {
                out.push_back(':');
            }
            append_number(out, seconds, 2);
        }
    }

    static void append_offset_x(std::string& out, int offset_seconds, int count, bool upper) {
        if (offset_seconds == 0 && upper) {
            out.push_back('Z');
            return;
        }
        char sign = '+';
        if (offset_seconds < 0) {
            sign = '-';
            offset_seconds = -offset_seconds;
        }
        const int hours = offset_seconds / 3600;
        const int minutes = (offset_seconds % 3600) / 60;
        const int seconds = offset_seconds % 60;
        out.push_back(sign);

        switch (count) {
        case 1:
            append_number(out, hours, 2);
            if (minutes != 0) {
                append_number(out, minutes, 2);
            }
            break;
        case 2:
            append_number(out, hours, 2);
            append_number(out, minutes, 2);
            break;
        case 3:
            append_number(out, hours, 2);
            out.push_back(':');
            append_number(out, minutes, 2);
            break;
        case 4:
            append_number(out, hours, 2);
            append_number(out, minutes, 2);
            if (seconds != 0) {
                append_number(out, seconds, 2);
            }
            break;
        case 5:
            append_number(out, hours, 2);
            out.push_back(':');
            append_number(out, minutes, 2);
            if (seconds != 0) {
                out.push_back(':');
                append_number(out, seconds, 2);
            }
            break;
        default:
            DCHECK(false) << "append_offset_x: X/x count must be 1..5, got " << count;
            break;
        }
    }

    static void append_localized_offset(std::string& out, int offset_seconds, bool full) {
        if (offset_seconds == 0) {
            out.append("GMT");
            return;
        }
        char sign = '+';
        if (offset_seconds < 0) {
            sign = '-';
            offset_seconds = -offset_seconds;
        }
        int hours = offset_seconds / 3600;
        int minutes = (offset_seconds % 3600) / 60;
        int seconds = offset_seconds % 60;
        out.append("GMT");
        out.push_back(sign);
        append_number(out, hours, full ? 2 : 1);
        if (minutes != 0 || seconds != 0 || full) {
            out.push_back(':');
            append_number(out, minutes, 2);
        }
        if (seconds != 0) {
            out.push_back(':');
            append_number(out, seconds, 2);
        }
    }

    static void append_zone_offset_by_kind(std::string& out, PatternKind kind, int count,
                                           int offset_seconds) {
        switch (kind) {
        case PatternKind::LocalizedOffset:
            append_localized_offset(out, offset_seconds, count == 4);
            return;
        case PatternKind::OffsetX:
            append_offset_x(out, offset_seconds, count, true);
            return;
        case PatternKind::Offsetx:
            append_offset_x(out, offset_seconds, count, false);
            return;
        case PatternKind::OffsetZ:
            if (count <= 3) {
                append_numeric_zone_offset(out, offset_seconds, false, false, false);
            } else if (count == 4) {
                append_localized_offset(out, offset_seconds, true);
            } else {
                append_numeric_zone_offset(out, offset_seconds, true, true, true);
            }
            return;
        default:
            return;
        }
    }

    static bool match_pattern_rules(char c, int count, std::span<const PatternRule> rules,
                                    PatternKind& kind) {
        for (const auto& rule : rules) {
            if (rule.matches(c, count)) {
                kind = rule.kind;
                return true;
            }
        }
        return false;
    }

    static bool append_date_item(const DateType& dt, const PatternItem& item, std::string& out) {
        switch (item.kind) {
        case PatternKind::Era:
            out.append(item.count == 4 ? ERA_NAMES_LONG[dt.year() > 0]
                                       : ERA_NAMES_SHORT[dt.year() > 0]);
            return true;
        case PatternKind::Year:
            append_year(out, dt.year(), item.count);
            return true;
        case PatternKind::DayOfYear:
            append_number(out, dt.day_of_year(), item.count == 1 ? 1 : item.count);
            return true;
        case PatternKind::MonthStandard:
        case PatternKind::MonthStandalone:
            append_month_value(out, dt.month(), item.count);
            return true;
        case PatternKind::DayOfMonth:
            append_number(out, dt.day(), item.count);
            return true;
        case PatternKind::QuarterStandard:
        case PatternKind::QuarterStandalone:
            append_quarter_value(out, dt.quarter(), item.count);
            return true;
        case PatternKind::DayOfWeekText:
            append_weekday_value(out, dt.weekday(), item.count);
            return true;
        case PatternKind::AlignedDayOfWeekInMonth:
            append_number(out, ((dt.day() - 1) % 7) + 1, 1);
            return true;
        default:
            return false;
        }
    }

    static bool append_time_item(const DateType& dt, const PatternItem& item, std::string& out) {
        switch (item.kind) {
        case PatternKind::AmPm:
            out.append(dt.hour() >= 12 ? "PM" : "AM");
            return true;
        case PatternKind::ClockHourOfAmPm:
            append_number(out, (dt.hour() % 12) == 0 ? 12 : (dt.hour() % 12), item.count);
            return true;
        case PatternKind::HourOfAmPm:
            append_number(out, dt.hour() % 12, item.count);
            return true;
        case PatternKind::ClockHourOfDay:
            append_number(out, dt.hour() == 0 ? 24 : dt.hour(), item.count);
            return true;
        case PatternKind::HourOfDay:
            append_number(out, dt.hour(), item.count);
            return true;
        case PatternKind::Minute:
            append_number(out, dt.minute(), item.count);
            return true;
        case PatternKind::Second:
            append_number(out, dt.second(), item.count);
            return true;
        case PatternKind::Fraction:
            append_fraction_value(out, dt.microsecond(), item.count);
            return true;
        default:
            return false;
        }
    }

    static bool append_zone_item(const DateType& dt, const PatternItem& item,
                                 const cctz::time_zone& time_zone, std::string& out) {
        auto tz_info = lookup_timezone_info(dt, time_zone);
        switch (item.kind) {
        case PatternKind::ZoneId:
            out.append(time_zone.name());
            return true;
        case PatternKind::ZoneName:
            if (item.count <= 3) {
                return append_timezone_display_name(time_zone, tz_info, false, out);
            }
            return append_timezone_display_name(time_zone, tz_info, true, out);
        case PatternKind::LocalizedOffset:
        case PatternKind::OffsetX:
        case PatternKind::Offsetx:
        case PatternKind::OffsetZ:
            append_zone_offset_by_kind(out, item.kind, item.count, tz_info.offset);
            return true;
        default:
            return false;
        }
    }

    static bool append_item(const DateType& dt, const PatternItem& item,
                            const cctz::time_zone& time_zone, std::string& out) {
        switch (get_pattern_category(item.kind)) {
        case PatternCategory::Literal:
            out.append(item.literal);
            return true;
        case PatternCategory::Date:
            return append_date_item(dt, item, out);
        case PatternCategory::Time:
            return append_time_item(dt, item, out);
        case PatternCategory::Zone:
            return append_zone_item(dt, item, time_zone, out);
        }
        return false;
    }

    static void append_literal_item(std::vector<PatternItem>& items, std::string literal) {
        items.push_back({PatternKind::Literal, 0, std::move(literal)});
    }

    static bool validate_spark_pattern_part(std::string_view s) {
        if (spark_pattern_part_has_forbidden_letters(s)) {
            return false;
        }
        return !spark_pattern_part_has_invalid_token_lengths(s);
    }

    static bool spark_pattern_part_has_forbidden_letters(std::string_view s) {
        static constexpr std::string_view kForbidden = "YWwuecABnNp";
        return std::ranges::find_first_of(s, kForbidden) != std::ranges::end(s);
    }

    static bool spark_pattern_part_has_invalid_token_lengths(std::string_view s) {
        static constexpr std::array<std::string_view, 7> kInvalidRuns {
                "GGGGG", "MMMMM", "LLLLL", "EEEEE", "QQQQQ", "qqqqq", "yyyyyyy"};
        return std::ranges::any_of(kInvalidRuns, [&](std::string_view bad) {
            return s.find(bad) != std::string_view::npos;
        });
    }

    template <typename Fn>
    static bool for_each_spark_quoted_segment(StringRef format_raw, Fn&& fn) {
        size_t part_begin = 0;
        bool in_pattern_segment = true;
        for (size_t i = 0; i <= format_raw.size; ++i) {
            if (i == format_raw.size || format_raw.data[i] == '\'') {
                std::string_view part {format_raw.data + part_begin, i - part_begin};
                const bool ended_by_quote = (i < format_raw.size);
                if (!fn(part, in_pattern_segment, ended_by_quote)) {
                    return false;
                }
                part_begin = i + 1;
                in_pattern_segment = !in_pattern_segment;
            }
        }
        return true;
    }

public:
    static bool prepare_format_string(StringRef format_raw, std::string& normalized) {
        bool era_in_pattern = false;
        if (!for_each_spark_quoted_segment(
                    format_raw, [&](std::string_view part, bool in_pattern, bool) {
                        if (in_pattern) {
                            if (!validate_spark_pattern_part(part)) {
                                return false;
                            }
                            era_in_pattern = era_in_pattern ||
                                             std::ranges::find(part, 'G') != std::ranges::end(part);
                        }
                        return true;
                    })) {
            return false;
        }

        normalized.clear();
        normalized.reserve(format_raw.size);
        const bool appended_ok = for_each_spark_quoted_segment(
                format_raw, [&](std::string_view part, bool in_pattern, bool ended_by_quote) {
                    if (in_pattern && !era_in_pattern) {
                        std::ranges::replace_copy(part, std::back_inserter(normalized), 'y', 'u');
                    } else {
                        normalized += part;
                    }
                    if (ended_by_quote) {
                        normalized += '\'';
                    }
                    return true;
                });
        DCHECK(appended_ok);
        return true;
    }

private:
    struct PatternParser {
        StringRef format;
        std::vector<PatternItem>& items;
        size_t pos = 0;
        int optional_depth = 0;

        bool parse() {
            if (format.size == 0) {
                return false;
            }
            while (pos < format.size) {
                if (!parse_next()) {
                    return false;
                }
            }
            return true;
        }

    private:
        static bool is_pattern_letter(char c) {
            return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
        }

        bool parse_next() {
            char c = format.data[pos];
            if (c == '\'') {
                return parse_quoted_literal();
            }
            if (c == '[') {
                ++optional_depth;
                ++pos;
                return true;
            }
            if (c == ']') {
                if (optional_depth == 0) {
                    return false;
                }
                --optional_depth;
                ++pos;
                return true;
            }
            if (is_pattern_letter(c)) {
                return parse_pattern_run();
            }
            DateFormatSparkImpl::append_literal_item(items, std::string(1, c));
            ++pos;
            return true;
        }

        bool parse_quoted_literal() {
            std::string literal;
            if (pos + 1 < format.size && format.data[pos + 1] == '\'') {
                DateFormatSparkImpl::append_literal_item(items, "'");
                pos += 2;
                return true;
            }

            ++pos;
            bool closed = false;
            while (pos < format.size) {
                if (format.data[pos] == '\'') {
                    if (pos + 1 < format.size && format.data[pos + 1] == '\'') {
                        literal.push_back('\'');
                        pos += 2;
                        continue;
                    }
                    ++pos;
                    closed = true;
                    break;
                }
                literal.push_back(format.data[pos++]);
            }

            if (!closed) {
                return false;
            }
            if (!literal.empty()) {
                DateFormatSparkImpl::append_literal_item(items, std::move(literal));
            }
            return true;
        }

        bool parse_pattern_run() {
            const char letter = format.data[pos];
            size_t next = pos + 1;
            while (next < format.size && format.data[next] == letter) {
                ++next;
            }

            const int count = static_cast<int>(next - pos);
            PatternKind kind = PatternKind::Literal;
            if (!std::ranges::any_of(DateFormatSparkImpl::kPatternRuleTables,
                                     [&](std::span<const PatternRule> rules) {
                                         return DateFormatSparkImpl::match_pattern_rules(
                                                 letter, count, rules, kind);
                                     })) {
                return false;
            }

            items.push_back({kind, count, {}});
            pos = next;
            return true;
        }
    };

    static bool get_or_parse_pattern_items(StringRef format,
                                           const std::vector<PatternItem>*& items) {
        static thread_local ParsedPatternCache cache;

        if (std::string_view(cache.format) != std::string_view(format.data, format.size)) {
            std::vector<PatternItem> parsed_items;
            PatternParser parser {format, parsed_items};
            if (!parser.parse()) {
                return false;
            }
            cache.format.assign(format.data, format.size);
            cache.items = std::move(parsed_items);
        }

        items = &cache.items;
        return true;
    }

    static bool format_date_time_spark(const DateType& dt, StringRef format,
                                       const cctz::time_zone& time_zone, std::string& out) {
        const std::vector<PatternItem>* cached_items = nullptr;
        if (!get_or_parse_pattern_items(format, cached_items)) {
            return false;
        }
        out.clear();
        out.reserve(format.size + 16);
        for (const auto& item : *cached_items) {
            if (!append_item(dt, item, time_zone, out)) {
                return false;
            }
        }
        return true;
    }
};

// date_format_spark keeps its own open/execute path so the primary
// FunctionDateTimeStringToString template stays free of Spark-only traits.
template <PrimitiveType P>
class FunctionDateTimeStringToString<DateFormatSparkImpl<P>> : public IFunction {
public:
    static constexpr auto name = DateFormatSparkImpl<P>::name;
    using Transform = DateFormatSparkImpl<P>;
    using ColumnType = typename PrimitiveTypeTraits<Transform::FromPType>::ColumnType;
    static_assert(!is_decimal(Transform::FromPType),
                  "date_format_spark only supports date/datetime inputs");

    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToString>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }

    struct FormatState {
        std::string format_str;
        std::string normalized_format_str;
        bool is_valid = true;
    };

    // Spark datetime patterns are often longer than Doris/MySQL style patterns like `%Y-%m-%d`,
    // especially when including:
    // - Time zone ID or offset tokens (VV / z / O / X / x / Z)
    // - Optional sections using `[...]`
    // - Literal quotes and combinations of longer pattern elements
    // Therefore, we allow a much larger upper limit here than traditional time format functions.
    static constexpr size_t MAX_SPARK_PATTERN_LENGTH = 512;

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        auto state = std::make_shared<FormatState>();
        DCHECK(context->get_num_args() == 2);
        context->set_function_state(scope, state);

        const auto* column_string = context->get_constant_col(1);
        if (column_string == nullptr) {
            return Status::InvalidArgument(
                    "The second parameter of the function {} must be a constant.", get_name());
        }

        auto string_value = column_string->column_ptr->get_data_at(0);
        if (string_value.data == nullptr) {
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        string_value = string_value.trim();
        auto format_str = StringRef(string_value.data, string_value.size);
        if (format_str.size > MAX_SPARK_PATTERN_LENGTH) {
            state->is_valid = false;
            return IFunction::open(context, scope);
        }

        state->format_str.assign(format_str.data, format_str.size);
        if (!Transform::prepare_format_string(format_str, state->normalized_format_str)) {
            state->is_valid = false;
        }

        return IFunction::open(context, scope);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    bool need_replace_null_data_to_default() const override { return true; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& sources =
                assert_cast<const ColumnType&>(*block.get_by_position(arguments[0]).column);

        auto col_res = ColumnString::create();
        RETURN_IF_ERROR(
                vector_constant(context, sources, col_res->get_chars(), col_res->get_offsets()));
        block.get_by_position(result).column = std::move(col_res);
        return Status::OK();
    }

    Status vector_constant(FunctionContext* context, const ColumnType& col,
                           ColumnString::Chars& res_data,
                           ColumnString::Offsets& res_offsets) const {
        auto* format_state = reinterpret_cast<FormatState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!format_state) {
            return Status::RuntimeError("function context for function '{}' must have FormatState;",
                                        get_name());
        }
        if (!format_state->is_valid) {
            throw_invalid_string(name, "invalid or oversized format");
        }

        StringRef format(format_state->normalized_format_str);
        const auto& pod_array = col.get_data();
        const auto len = pod_array.size();
        res_offsets.resize(len);
        res_data.reserve(len * (format.size + 16));

        size_t offset = 0;
        for (int i = 0; i < len; ++i) {
            bool invalid = Transform::execute(pod_array[i], format, res_data, offset,
                                              context->state()->timezone_obj());
            if (invalid) [[unlikely]] {
                char buf[64];
                pod_array[i].to_string(buf);
                throw_invalid_strings(name, buf, format_state->format_str);
            }
            res_offsets[i] = cast_set<uint32_t>(offset);
        }
        res_data.resize(offset);
        return Status::OK();
    }
};

} // namespace doris::vectorized
