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

#include <unicode/gregocal.h>
#include <unicode/locid.h>
#include <unicode/stringpiece.h>
#include <unicode/timezone.h>
#include <unicode/unistr.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized::detail {

namespace spark_legacy_date_format_internal {

enum class PatternKind {
    Literal,
    Era,
    Year,
    WeekYear,
    DayOfYear,
    MonthStandard,
    MonthStandalone,
    WeekOfYear,
    WeekOfMonth,
    DayOfMonth,
    DayOfWeekText,
    DayOfWeekNumber,
    DayOfWeekInMonth,
    AmPm,
    ClockHourOfAmPm,
    HourOfAmPm,
    ClockHourOfDay,
    HourOfDay,
    Minute,
    Second,
    Fraction,
    ZoneName,
    OffsetX,
    OffsetZ
};

struct PatternItem {
    PatternKind kind;
    int count;
    std::string literal;
};

struct CompiledPatternData {
    std::vector<PatternItem> items;
    bool needs_legacy_calendar = false;
    bool needs_zone_name = false;
};

struct LegacyCalendarFields {
    int week_year;
    int week_of_year;
    int week_of_month;
    int day_number_of_week;
};

} // namespace spark_legacy_date_format_internal

template <PrimitiveType PType>
class SparkLegacyDateFormatEngine {
public:
    using DateType = typename PrimitiveTypeTraits<PType>::CppType;

    struct CompiledPattern {
        std::shared_ptr<const spark_legacy_date_format_internal::CompiledPatternData> data;
    };

    struct SharedRuntimeContext {
        const cctz::time_zone* time_zone = nullptr;
        std::unique_ptr<icu::TimeZone> icu_time_zone;
        std::unique_ptr<icu::GregorianCalendar> calendar;
    };

    static bool compile_format_string(StringRef format_raw, CompiledPattern& compiled_pattern) {
        auto compiled = std::make_shared<CompiledPatternData>();
        PatternParser parser {format_raw, *compiled};
        if (!parser.parse()) {
            return false;
        }
        compiled_pattern.data = std::move(compiled);
        return true;
    }

    static std::unique_ptr<SharedRuntimeContext> create_shared_runtime_context(
            const CompiledPattern& compiled_pattern, const cctz::time_zone& time_zone) {
        if (!compiled_pattern.data) {
            return nullptr;
        }

        auto runtime_context = std::make_unique<SharedRuntimeContext>();
        runtime_context->time_zone = &time_zone;
        if (compiled_pattern.data->needs_legacy_calendar ||
            compiled_pattern.data->needs_zone_name) {
            runtime_context->icu_time_zone = create_icu_timezone(time_zone);
            if (!runtime_context->icu_time_zone) {
                return nullptr;
            }
        }
        return runtime_context;
    }

    static bool execute(const DateType& dt, const CompiledPattern& compiled_pattern,
                        ColumnString::Chars& res_data, size_t& offset,
                        SharedRuntimeContext& runtime_context) {
        if (!compiled_pattern.data) {
            return true;
        }

        std::string formatted;
        if (!format_date_time(dt, *compiled_pattern.data, runtime_context, formatted)) {
            return true;
        }
        res_data.insert(formatted.data(), formatted.data() + formatted.size());
        offset += formatted.size();
        return false;
    }

private:
    using PatternKind = spark_legacy_date_format_internal::PatternKind;
    using PatternItem = spark_legacy_date_format_internal::PatternItem;
    using CompiledPatternData = spark_legacy_date_format_internal::CompiledPatternData;
    using LegacyCalendarFields = spark_legacy_date_format_internal::LegacyCalendarFields;

    class PatternParser;

    struct FormatRuntimeContext {
        SharedRuntimeContext& shared;
        std::optional<LegacyCalendarFields> calendar_fields;
        std::optional<cctz::time_zone::absolute_lookup> tz_info;
    };

    struct PatternRule {
        char letter;
        int min_count;
        int max_count;
        PatternKind kind;

        [[nodiscard]] constexpr bool matches(char c, int run_len) const noexcept {
            return c == letter && run_len >= min_count && run_len <= max_count;
        }
    };

private:
    static constexpr std::array<std::string_view, 12> MONTH_NAMES_SHORT = {
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    static constexpr std::array<std::string_view, 12> MONTH_NAMES_LONG = {
            "January", "February", "March",     "April",   "May",      "June",
            "July",    "August",   "September", "October", "November", "December"};
    static constexpr std::array<std::string_view, 7> DAY_NAMES_SHORT = {"Mon", "Tue", "Wed", "Thu",
                                                                        "Fri", "Sat", "Sun"};
    static constexpr std::array<std::string_view, 7> DAY_NAMES_LONG = {
            "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    static constexpr std::array<std::string_view, 2> ERA_NAMES = {"BC", "AD"};
    static constexpr int MAX_PATTERN_RUN_LENGTH = 512;
    static constexpr std::array<PatternRule, 9> DATE_PATTERN_RULES = {{
            {'G', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::Era},
            {'y', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::Year},
            {'Y', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::WeekYear},
            {'D', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::DayOfYear},
            {'M', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::MonthStandard},
            {'L', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::MonthStandalone},
            {'w', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::WeekOfYear},
            {'W', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::WeekOfMonth},
            {'d', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::DayOfMonth},
    }};
    static constexpr std::array<PatternRule, 5> TEXT_PATTERN_RULES = {{
            {'E', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::DayOfWeekText},
            {'F', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::DayOfWeekInMonth},
            {'u', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::DayOfWeekNumber},
            {'a', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::AmPm},
            {'z', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::ZoneName},
    }};
    static constexpr std::array<PatternRule, 7> TIME_PATTERN_RULES = {{
            {'h', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::ClockHourOfAmPm},
            {'K', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::HourOfAmPm},
            {'k', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::ClockHourOfDay},
            {'H', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::HourOfDay},
            {'m', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::Minute},
            {'s', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::Second},
            {'S', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::Fraction},
    }};
    static constexpr std::array<PatternRule, 2> ZONE_PATTERN_RULES = {{
            {'X', 1, 3, PatternKind::OffsetX},
            {'Z', 1, MAX_PATTERN_RUN_LENGTH, PatternKind::OffsetZ},
    }};
    static constexpr std::array<std::span<const PatternRule>, 4> PATTERN_RULE_TABLES {
            std::span<const PatternRule>(DATE_PATTERN_RULES),
            std::span<const PatternRule>(TEXT_PATTERN_RULES),
            std::span<const PatternRule>(TIME_PATTERN_RULES),
            std::span<const PatternRule>(ZONE_PATTERN_RULES),
    };

    class PatternParser {
    public:
        PatternParser(StringRef format, CompiledPatternData& compiled_pattern)
                : _format(format), _compiled_pattern(compiled_pattern) {}

        bool parse() {
            while (_pos < _format.size) {
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
            char c = _format.data[_pos];
            if (c == '\'') {
                return parse_quoted_literal();
            }
            if (is_pattern_letter(c)) {
                return parse_pattern_run();
            }
            append_literal_item(_compiled_pattern, std::string(1, c));
            ++_pos;
            return true;
        }

        bool parse_quoted_literal() {
            std::string literal;
            if (_pos + 1 < _format.size && _format.data[_pos + 1] == '\'') {
                append_literal_item(_compiled_pattern, "'");
                _pos += 2;
                return true;
            }

            ++_pos;
            bool closed = false;
            while (_pos < _format.size) {
                if (_format.data[_pos] == '\'') {
                    if (_pos + 1 < _format.size && _format.data[_pos + 1] == '\'') {
                        literal.push_back('\'');
                        _pos += 2;
                        continue;
                    }
                    ++_pos;
                    closed = true;
                    break;
                }
                literal.push_back(_format.data[_pos++]);
            }

            if (!closed) {
                return false;
            }
            if (!literal.empty()) {
                append_literal_item(_compiled_pattern, std::move(literal));
            }
            return true;
        }

        bool parse_pattern_run() {
            const char letter = _format.data[_pos];
            size_t next = _pos + 1;
            while (next < _format.size && _format.data[next] == letter) {
                ++next;
            }

            const int count = static_cast<int>(next - _pos);
            auto kind = find_pattern_kind(letter, count);
            if (!kind.has_value()) {
                return false;
            }

            _compiled_pattern.items.push_back({*kind, count, {}});
            _compiled_pattern.needs_legacy_calendar =
                    _compiled_pattern.needs_legacy_calendar || pattern_uses_legacy_calendar(*kind);
            _compiled_pattern.needs_zone_name =
                    _compiled_pattern.needs_zone_name || *kind == PatternKind::ZoneName;
            _pos = next;
            return true;
        }

        StringRef _format;
        CompiledPatternData& _compiled_pattern;
        size_t _pos = 0;
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

    static void append_weekday_value(std::string& out, int weekday, int count) {
        out.append(count >= 4 ? DAY_NAMES_LONG[weekday] : DAY_NAMES_SHORT[weekday]);
    }

    static void append_fraction_value(std::string& out, int microsecond, int count) {
        std::string fraction = std::to_string(microsecond / 1000);
        if (fraction.size() < static_cast<size_t>(count)) {
            fraction.insert(fraction.begin(), count - fraction.size(), '0');
        }
        out.append(fraction);
    }

    static bool pattern_uses_legacy_calendar(PatternKind kind) {
        switch (kind) {
        case PatternKind::WeekYear:
        case PatternKind::WeekOfYear:
        case PatternKind::WeekOfMonth:
        case PatternKind::DayOfWeekNumber:
            return true;
        default:
            return false;
        }
    }

    static std::optional<PatternKind> find_pattern_kind(char c, int count) {
        const auto matches = [&](const PatternRule& rule) { return rule.matches(c, count); };
        for (std::span<const PatternRule> rules : PATTERN_RULE_TABLES) {
            if (auto iter = std::ranges::find_if(rules, matches); iter != rules.end()) {
                return iter->kind;
            }
        }
        return std::nullopt;
    }

    static void append_literal_item(CompiledPatternData& compiled_pattern, std::string literal) {
        compiled_pattern.items.push_back({PatternKind::Literal, 0, std::move(literal)});
    }

    static cctz::time_zone::absolute_lookup lookup_timezone_info(const DateType& dt,
                                                                 const cctz::time_zone& time_zone) {
        int64_t ts = 0;
        dt.unix_timestamp(&ts, time_zone);
        auto tp = std::chrono::system_clock::from_time_t(ts);
        return time_zone.lookup(tp);
    }

    static std::unique_ptr<icu::TimeZone> create_icu_timezone(const cctz::time_zone& time_zone) {
        auto icu_timezone = std::unique_ptr<icu::TimeZone> {icu::TimeZone::createTimeZone(
                icu::UnicodeString::fromUTF8(icu::StringPiece(time_zone.name())))};
        if (*icu_timezone == icu::TimeZone::getUnknown()) {
            return nullptr;
        }
        return icu_timezone;
    }

    static bool ensure_icu_timezone(FormatRuntimeContext& context) {
        if (context.shared.icu_time_zone) {
            return true;
        }

        context.shared.icu_time_zone = create_icu_timezone(*context.shared.time_zone);
        return context.shared.icu_time_zone != nullptr;
    }

    static bool ensure_calendar(FormatRuntimeContext& context) {
        if (context.shared.calendar) {
            return true;
        }
        if (!ensure_icu_timezone(context)) {
            return false;
        }

        UErrorCode status = U_ZERO_ERROR;
        auto calendar = std::make_unique<icu::GregorianCalendar>(*context.shared.icu_time_zone,
                                                                 icu::Locale::getUS(), status);
        if (U_FAILURE(status)) {
            return false;
        }
        context.shared.calendar = std::move(calendar);
        return true;
    }

    static bool fill_legacy_calendar_fields(const DateType& dt, FormatRuntimeContext& context,
                                            LegacyCalendarFields& fields) {
        if (!ensure_calendar(context)) {
            return false;
        }

        int64_t ts = 0;
        dt.unix_timestamp(&ts, *context.shared.time_zone);
        const auto unix_millis = static_cast<UDate>(ts) * 1000.0 + (dt.microsecond() / 1000);
        UErrorCode status = U_ZERO_ERROR;
        context.shared.calendar->setTime(unix_millis, status);
        if (U_FAILURE(status)) {
            return false;
        }

        fields.week_year = context.shared.calendar->get(UCAL_YEAR_WOY, status);
        fields.week_of_year = context.shared.calendar->get(UCAL_WEEK_OF_YEAR, status);
        fields.week_of_month = context.shared.calendar->get(UCAL_WEEK_OF_MONTH, status);
        const int day_of_week = context.shared.calendar->get(UCAL_DAY_OF_WEEK, status);
        if (U_FAILURE(status)) {
            return false;
        }
        fields.day_number_of_week = ((day_of_week + 5) % 7) + 1;
        return true;
    }

    static bool ensure_legacy_calendar_fields(const DateType& dt, FormatRuntimeContext& context) {
        if (context.calendar_fields.has_value()) {
            return true;
        }

        LegacyCalendarFields fields {};
        if (!fill_legacy_calendar_fields(dt, context, fields)) {
            return false;
        }
        context.calendar_fields.emplace(fields);
        return true;
    }

    static bool ensure_timezone_info(const DateType& dt, FormatRuntimeContext& context) {
        if (context.tz_info.has_value()) {
            return true;
        }

        context.tz_info.emplace(lookup_timezone_info(dt, *context.shared.time_zone));
        return true;
    }

    static bool append_timezone_display_name(FormatRuntimeContext& context,
                                             const cctz::time_zone::absolute_lookup& tz_info,
                                             bool long_name, std::string& out) {
        if (!ensure_icu_timezone(context)) {
            return false;
        }
        icu::UnicodeString display;
        context.shared.icu_time_zone->getDisplayName(
                static_cast<UBool>(tz_info.is_dst),
                long_name ? icu::TimeZone::LONG : icu::TimeZone::SHORT, icu::Locale::getUS(),
                display);
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
        const int hours = offset_seconds / 3600;
        const int minutes = (offset_seconds % 3600) / 60;
        const int seconds = offset_seconds % 60;
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
        default:
            DCHECK(false) << "append_offset_x: X count must be 1..3, got " << count;
            break;
        }
    }

    static bool append_date_item(const DateType& dt, const PatternItem& item,
                                 FormatRuntimeContext& context, std::string& out) {
        switch (item.kind) {
        case PatternKind::Era:
            out.append(ERA_NAMES[dt.year() > 0]);
            return true;
        case PatternKind::Year:
            append_year(out, dt.year(), item.count);
            return true;
        case PatternKind::WeekYear:
            if (!ensure_legacy_calendar_fields(dt, context)) {
                return false;
            }
            append_year(out, context.calendar_fields->week_year, item.count);
            return true;
        case PatternKind::DayOfYear:
            append_number(out, dt.day_of_year(), item.count == 1 ? 1 : item.count);
            return true;
        case PatternKind::MonthStandard:
        case PatternKind::MonthStandalone:
            append_month_value(out, dt.month(), item.count);
            return true;
        case PatternKind::WeekOfYear:
            if (!ensure_legacy_calendar_fields(dt, context)) {
                return false;
            }
            append_number(out, context.calendar_fields->week_of_year, item.count);
            return true;
        case PatternKind::WeekOfMonth:
            if (!ensure_legacy_calendar_fields(dt, context)) {
                return false;
            }
            append_number(out, context.calendar_fields->week_of_month, item.count);
            return true;
        case PatternKind::DayOfMonth:
            append_number(out, dt.day(), item.count);
            return true;
        case PatternKind::DayOfWeekText:
            append_weekday_value(out, dt.weekday(), item.count);
            return true;
        case PatternKind::DayOfWeekNumber:
            if (!ensure_legacy_calendar_fields(dt, context)) {
                return false;
            }
            append_number(out, context.calendar_fields->day_number_of_week, item.count);
            return true;
        case PatternKind::DayOfWeekInMonth:
            append_number(out, ((dt.day() - 1) / 7) + 1, item.count);
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
                                 FormatRuntimeContext& context, std::string& out) {
        if (!ensure_timezone_info(dt, context)) {
            return false;
        }
        switch (item.kind) {
        case PatternKind::ZoneName:
            if (item.count <= 3) {
                return append_timezone_display_name(context, *context.tz_info, false, out);
            }
            return append_timezone_display_name(context, *context.tz_info, true, out);
        case PatternKind::OffsetX:
            append_offset_x(out, context.tz_info->offset, item.count, true);
            return true;
        case PatternKind::OffsetZ:
            append_numeric_zone_offset(out, context.tz_info->offset, false, false, false);
            return true;
        default:
            return false;
        }
    }

    static bool append_item(const DateType& dt, const PatternItem& item,
                            FormatRuntimeContext& context, std::string& out) {
        switch (item.kind) {
        case PatternKind::Literal:
            out.append(item.literal);
            return true;
        case PatternKind::Era:
        case PatternKind::Year:
        case PatternKind::WeekYear:
        case PatternKind::DayOfYear:
        case PatternKind::MonthStandard:
        case PatternKind::MonthStandalone:
        case PatternKind::WeekOfYear:
        case PatternKind::WeekOfMonth:
        case PatternKind::DayOfMonth:
        case PatternKind::DayOfWeekText:
        case PatternKind::DayOfWeekNumber:
        case PatternKind::DayOfWeekInMonth:
            return append_date_item(dt, item, context, out);
        case PatternKind::AmPm:
        case PatternKind::ClockHourOfAmPm:
        case PatternKind::HourOfAmPm:
        case PatternKind::ClockHourOfDay:
        case PatternKind::HourOfDay:
        case PatternKind::Minute:
        case PatternKind::Second:
        case PatternKind::Fraction:
            return append_time_item(dt, item, out);
        case PatternKind::ZoneName:
        case PatternKind::OffsetX:
        case PatternKind::OffsetZ:
            return append_zone_item(dt, item, context, out);
        }
        return false;
    }

    static bool format_date_time(const DateType& dt, const CompiledPatternData& compiled_pattern,
                                 SharedRuntimeContext& runtime_context, std::string& out) {
        FormatRuntimeContext context {runtime_context, std::nullopt, std::nullopt};
        out.clear();
        out.reserve(compiled_pattern.items.size() * 8);
        for (const auto& item : compiled_pattern.items) {
            if (!append_item(dt, item, context, out)) {
                return false;
            }
        }
        return true;
    }
};

} // namespace doris::vectorized::detail
