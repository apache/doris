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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("doc_date_functions_test") {
    //FIXME: make FE and BE have same range of timezone
    // sql "set debug_skip_fold_constant=true;"
    // Test Group 1: Basic Date Functions(1 - 12)
    
    sql """ set time_zone = '+08:00'; """

    /**
     * Use datetime version to validate timestamptz ceil/floor functions
     * Eg. res of YEAR_CEIL('2025-12-31 23:59:59+05:00') should equal to YEAR_CEIL('2026-01-01 02:59:59')
     * If all time-related parameters are timestamptz, then the res of timestamptz version (without timezone) should equal to datetime version()
     * E.g., 2027-01-01 00:00:00+08:00 VS 2027-01-01 00:00:00
     * Else if mixed types (some timestamptz, some datetime), then the res of timestamptz version should equal to datetime version directly
     */
    def validateTimestamptzCeilFloor = { sqlWithTz, timeZone = '+08:00' ->
        def originalTimeZone = null
        try {
            def tzResult = sql "SELECT @@time_zone"
            originalTimeZone = tzResult[0][0]
            sql "set time_zone = '${timeZone}'"

            sqlWithTz = sqlWithTz.replaceAll(/;\s*$/, '').trim()

            def matcher = sqlWithTz =~ /(\w+)\s*\((.*)\)/
            if (!matcher) {
                logger.error("Failed to parse SQL: ${sqlWithTz}")
                return false
            }

            def funcName = matcher[0][1]
            def params = matcher[0][2].split(',').collect { it.trim() }
            
            def timeParams = params.findAll { param ->
                param =~ /^(['"]).*\1$/
            }
            
            def allTimestamptz = timeParams.size() > 0 && timeParams.every { param ->
                param =~ /[+-]\d{2}:\d{2}['"]?\s*$/ || param =~ /Z['"]?\s*$/
            }
            
            // Convert all timestamptz parameters to datetime in current timezone
            def convertedParams = params.collect { param ->
                if (param =~ /[+-]\d{2}:\d{2}['"]?\s*$/ || param =~ /Z['"]?\s*$/) {
                    def tzMatcher = param =~ /'([^']+)'/
                    if (tzMatcher) {
                        def tzValue = tzMatcher[0][1]
                        def convertedResult = sql("SELECT CAST(CAST('${tzValue}' AS DATETIME) AS STRING)")[0][0]
                        return "'${convertedResult}'"
                    }
                }
                return param
            }
            
            def resTz = sql("SELECT CAST((${sqlWithTz}) AS STRING)")[0][0]
            def sqlWithDt = "SELECT CAST((${funcName}(${convertedParams.join(', ')})) AS STRING)"
            logger.info("Datetime query: ${sqlWithDt}")
            def resDttm = sql(sqlWithDt)[0][0]
            
            // Validate based on allTimestamptz
            def resTzStr = resTz.toString()
            def resDttmStr = resDttm.toString()
            
            if (allTimestamptz) {
                // Case 1: All time parameters are timestamptz
                // Validate: resTz (without timezone) should equal resDttm
                if (!resTzStr.replaceAll(/[+-]\d{2}:\d{2}$/, '').trim().equals(resDttmStr)) {
                    logger.error("Validation failed for all-timestamptz: ${sqlWithTz}")
                    logger.error("Expected (datetime): ${resDttmStr}")
                    logger.error("Got (timestamptz): ${resTzStr}")
                    return false
                }
                logger.info("All timestamptz validation passed: resTz=${resTzStr}, resDttm=${resDttmStr}")
            } else {
                // Case 2: Mixed types (some timestamptz, some datetime)
                // Validate: resTz should equal resDttm directly
                if (!resTzStr.equals(resDttmStr)) {
                    logger.error("Validation failed for mixed-types: ${sqlWithTz}")
                    logger.error("Expected: ${resDttmStr}")
                    logger.error("Got: ${resTzStr}")
                    return false
                }
                logger.info("Mixed types validation passed: resTz=${resTzStr}, resDttm=${resDttmStr}")
            }
            
            return true
        } finally {
            if (originalTimeZone != null) {
                sql "set time_zone = '${originalTimeZone}'"
            }
        }
    }

    /**
     * Use datetime version to validate timestamptz DATE_TRUNC function
     * The first parameter can be timestamptz type, and we need to verify:
     * resTz should equal resDttm + '+08:00'
     * E.g., "2019-01-01 00:00:00+08:00" should equal "2019-01-01 00:00:00" + '+08:00'
     */
    def validateTimestamptzTrunc = { sqlWithTz, timeZone = '+08:00' ->
        def originalTimeZone = null
        try {
            def tzResult = sql "SELECT @@time_zone"
            originalTimeZone = tzResult[0][0]
            sql "set time_zone = '${timeZone}'"
            
            sqlWithTz = sqlWithTz.replaceAll(/;\s*$/, '').trim()
            
            // Extract function name and parameters
            def matcher = sqlWithTz =~ /(\w+)\s*\((.*)\)/
            if (!matcher) {
                logger.error("Failed to parse SQL: ${sqlWithTz}")
                return false
            }
            
            def funcName = matcher[0][1]
            def params = matcher[0][2].split(',').collect { it.trim() }
            
            // Check if first parameter exists and is timestamptz
            if (params.size() < 1) {
                logger.error("No parameters found in: ${sqlWithTz}")
                return false
            }
            
            def firstParam = params[0]
            def isTimestamptz = firstParam =~ /[+-]\d{2}:\d{2}['"]?\s*$/ || firstParam =~ /Z['"]?\s*$/
            
            if (!isTimestamptz) {
                logger.info("First parameter is not timestamptz, skipping validation: ${sqlWithTz}")
                return true
            }
            
            // Convert first parameter to datetime in current timezone
            def tzMatcher = firstParam =~ /'([^']+)'/
            if (!tzMatcher) {
                logger.error("Failed to extract timestamptz value from: ${firstParam}")
                return false
            }
            
            def tzValue = tzMatcher[0][1]
            def convertedResult = sql("SELECT CAST(CAST('${tzValue}' AS DATETIME) AS STRING)")[0][0]
            
            // Build datetime query with converted first parameter
            def convertedParams = ["'${convertedResult}'"] + params.drop(1)
            
            // Execute both queries
            def resTz = sql("SELECT CAST((${sqlWithTz}) AS STRING)")[0][0]
            def sqlWithDt = "SELECT CAST((${funcName}(${convertedParams.join(', ')})) AS STRING)"
            logger.info("Datetime query: ${sqlWithDt}")
            def resDttm = sql(sqlWithDt)[0][0]
            
            // Validate: resTz should equal resDttm + timezone
            def resTzStr = resTz.toString()
            def resDttmStr = resDttm.toString()
            
            def expectedTz = "${resDttmStr}${timeZone}"
            
            if (!resTzStr.equals(expectedTz)) {
                logger.error("Validation failed for DATE_TRUNC with timestamptz: ${sqlWithTz}")
                logger.error("Expected: ${expectedTz}")
                logger.error("Got: ${resTzStr}")
                logger.error("Datetime result: ${resDttmStr}")
                return false
            }
            
            logger.info("DATE_TRUNC timestamptz validation passed: resTz=${resTzStr}, resDttm=${resDttmStr}")
            return true
        } finally {
            if (originalTimeZone != null) {
                sql "set time_zone = '${originalTimeZone}'"
            }
        }
    }

    // 1. CONVERT_TZ function tests
    // Convert China Shanghai time to America Los Angeles
    qt_convert_tz_1 """select CONVERT_TZ(CAST('2019-08-01 13:21:03' AS DATETIME), 'Asia/Shanghai', 'America/Los_Angeles')"""
    
    // Convert East 8 zone (+08:00) time to America Los Angeles
    qt_convert_tz_2 """select CONVERT_TZ(CAST('2019-08-01 13:21:03' AS DATETIME), '+08:00', 'America/Los_Angeles')"""
    
    // Input is date type, time part is automatically converted to 00:00:00
    qt_convert_tz_3 """select CONVERT_TZ(CAST('2019-08-01 13:21:03' AS DATE), 'Asia/Shanghai', 'America/Los_Angeles')"""
    
    // Convert time is NULL, output NULL
    qt_convert_tz_4 """select CONVERT_TZ(NULL, 'Asia/Shanghai', 'America/New_York')"""
    
    // Any timezone is NULL, return NULL
    qt_convert_tz_5 """select CONVERT_TZ('2019-08-01 13:21:03', NULL, 'America/Los_Angeles')"""
    qt_convert_tz_6 """select CONVERT_TZ('2019-08-01 13:21:03', '+08:00', NULL)"""
    
    test {
        sql """SELECT CONVERT_TZ('2038-01-19 03:14:07','GMTaa','MET')"""
        exception "Operation convert_tz invalid timezone"
    }
    
    // Time with scale
    qt_convert_tz_8 """select CONVERT_TZ('2019-08-01 13:21:03.636', '+08:00', 'America/Los_Angeles')"""

    // test {
    //     sql """select CONVERT_TZ('2019-08-01 13:21:03', '+08:00', '+15:00')"""
    //     exception "Operation convert_tz invalid timezone"
    // }

    // 5. DATE_ADD function tests
    // Add days
    qt_date_add_1 """select date_add(cast('2010-11-30 23:59:59' as datetime), INTERVAL 2 DAY)"""
    
    // Add quarter
    qt_date_add_2 """select DATE_ADD(cast('2023-01-01' as date), INTERVAL 1 QUARTER)"""
    
    // Add weeks
    qt_date_add_3 """select DATE_ADD('2023-01-01', INTERVAL 1 WEEK)"""
    
    // Add month, because February 2023 only has 28 days, so January 31 plus one month returns February 28
    qt_date_add_4 """select DATE_ADD('2023-01-31', INTERVAL 1 MONTH)"""
    
    // Negative number test
    qt_date_add_5 """select DATE_ADD('2019-01-01', INTERVAL -3 DAY)"""
    
    // Cross-year hour increment
    qt_date_add_6 """select DATE_ADD('2023-12-31 23:00:00', INTERVAL 2 HOUR)"""
    
    // Parameter is NULL, return NULL
    qt_date_add_7 """select DATE_ADD(NULL, INTERVAL 1 MONTH)"""
    qt_date_add_8 """select DATE_ADD('2023-12-31 23:59:59+08:00', INTERVAL 1 YEAR)"""
    qt_date_add_9 """select DATE_ADD('2023-06-15 12:30:45-05:00', INTERVAL 2 QUARTER)"""
    qt_date_add_10 """select DATE_ADD('2023-01-31 10:15:30+00:00', INTERVAL 3 MONTH)"""
    qt_date_add_11 """select DATE_ADD('2024-02-29 15:45:22+09:00', INTERVAL 1 WEEK)"""
    qt_date_add_12 """select DATE_ADD('2023-12-25 08:00:00-08:00', INTERVAL 5 DAY)"""
    qt_date_add_13 """select DATE_ADD('2023-01-01 00:00:00Z', INTERVAL 10 HOUR)"""
    qt_date_add_14 """select DATE_ADD('2023-06-15 23:59:59+05:30', INTERVAL 45 MINUTE)"""
    qt_date_add_15 """select DATE_ADD('2023-12-31 23:59:59.999999+08:00', INTERVAL 1 SECOND)"""
    qt_date_add_16 """select DATE_ADD('0001-01-01 00:00:00Z', INTERVAL '1 5:30:45' DAY_SECOND)"""
    qt_date_add_17 """select DATE_ADD('9999-12-28 12:00:00+03:00', INTERVAL '2 10' DAY_HOUR)"""
    qt_date_add_18 """select DATE_ADD('2023-06-15 15:45:30-07:00', INTERVAL '30:45' MINUTE_SECOND)"""
    qt_date_add_19 """select DATE_ADD('2024-01-15 10:20:30.123456+01:00', INTERVAL '2.500000' SECOND_MICROSECOND)"""
    qt_date_add_20 """select DATE_ADD('2023-03-15 14:30:00+11:00', INTERVAL -1 YEAR)"""
    qt_date_add_21 """select DATE_ADD('2024-12-31 23:59:59-12:00', INTERVAL -5 DAY)"""
    qt_date_add_22 """select DATE_ADD('2023-07-04 16:20:15+00:00', INTERVAL -2 HOUR)"""
    qt_date_add_23 """select DATE_ADD('2025-02-28 18:45:22+06:00', INTERVAL -30 MINUTE)"""
    qt_date_add_24 """select DATE_ADD('2023-11-11 11:11:11.111111-03:00', INTERVAL -1 SECOND)"""
    qt_date_add_25 """select date_add('2023-02-28 16:00:00 UTC', INTERVAL 1 DAY)"""
    qt_date_add_26 """select date_add('2023-02-28 16:00:00 America/New_York', INTERVAL 1 DAY)"""
    qt_date_add_27 """select date_add('2023-02-28 16:00:00UTC', INTERVAL 1 DAY)"""
    qt_date_add_28 """select date_add('2023-02-28 16:00:00America/New_York', INTERVAL 1 DAY)"""
    qt_date_add_29 """select date_add('2023-03-12 01:30:00 Europe/London', INTERVAL 1 DAY)"""
    qt_date_add_30 """select date_add('2023-11-05 01:30:00 America/New_York', INTERVAL 1 DAY)"""

    // 6. DATE_CEIL function tests  
    // Round up seconds to five-second intervals
    qt_date_ceil_1 """select date_ceil(cast('2023-07-13 22:28:18' as datetime),interval 5 second)"""
    
    // Datetime parameter with scale
    qt_date_ceil_2 """select date_ceil(cast('2023-07-13 22:28:18.123' as datetime(3)),interval 5 second)"""
    
    // Round up to five-minute intervals
    qt_date_ceil_3 """select date_ceil('2023-07-13 22:28:18',interval 5 minute)"""
    
    // Round up to five-week intervals
    qt_date_ceil_4 """select date_ceil('2023-07-13 22:28:18',interval 5 WEEK)"""
    
    // Round up to five-hour intervals
    qt_date_ceil_5 """select date_ceil('2023-07-13 22:28:18',interval 5 hour)"""
    
    // Round up to five-day intervals
    qt_date_ceil_6 """select date_ceil('2023-07-13 22:28:18',interval 5 day)"""
    
    // Round up to five-month intervals
    qt_date_ceil_7 """select date_ceil('2023-07-13 22:28:18',interval 5 month)"""
    
    // Round up to five-year intervals
    qt_date_ceil_8 """select date_ceil('2023-07-13 22:28:18',interval 5 year)"""
    
    // Any parameter is NULL
    qt_date_ceil_9 """select date_ceil('9900-07-13',interval NULL year)"""
    qt_date_ceil_10 """select date_ceil(NULL,interval 5 year)"""

    validateTimestamptzCeilFloor("select date_ceil('2023-03-15 14:25:38.999999+02:00', interval 1 second)", '+02:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-06-20 23:59:59.123456-04:00', interval 5 second)", '-04:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-06-20 09:59:45+05:00', interval 3 minute)", '+05:00')
    validateTimestamptzCeilFloor("select date_ceil('2023-11-08 23:58:30+09:00', interval 5 minute)", '+09:00')
    validateTimestamptzCeilFloor("select date_ceil('2023-11-08 23:35:12-05:00', interval 2 hour)", '-05:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-02-29 22:45:30Z', interval 3 hour)", '+00:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-01-31 20:18:45+01:00', interval 2 day)", '+01:00')
    validateTimestamptzCeilFloor("select date_ceil('2023-12-30 22:30:15+05:30', interval 3 day)", '+05:30')
    validateTimestamptzCeilFloor("select date_ceil('2023-09-10 23:22:33-07:00', interval 2 week)", '-07:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-12-29 20:15:40+03:00', interval 1 week)", '+03:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-01-31 22:41:56-06:00', interval 2 month)", '-06:00')
    validateTimestamptzCeilFloor("select date_ceil('2023-12-15 21:30:00+02:00', interval 3 month)", '+02:00')
    validateTimestamptzCeilFloor("select date_ceil('2024-12-31 20:52:27-08:00', interval 2 year)", '-08:00')
    validateTimestamptzCeilFloor("select date_ceil('2023-11-15 22:30:15+01:00', interval 3 year)", '+01:00')

    // 7. DATEDIFF function tests
    // Two dates differ by 1 day (ignore time part)
    qt_datediff_1 """select datediff(CAST('2007-12-31 23:59:59' AS DATETIME), CAST('2007-12-30' AS DATETIME))"""
    
    // First date is earlier than second date, return negative number
    qt_datediff_2 """select datediff(CAST('2010-11-30 23:59:59' AS DATETIME), CAST('2010-12-31' AS DATETIME))"""
    
    // Any parameter is NULL
    qt_datediff_3 """select datediff('2023-01-01', NULL)"""
    
    // If input is datetime type, time part will be ignored
    qt_datediff_4 """select datediff('2023-01-02 13:00:00', '2023-01-01 12:00:00')"""
    qt_datediff_5 """select datediff('2023-01-02 12:00:00', '2023-01-01 13:00:00')"""

    // 8. DATE_FLOOR function tests
    // Round down to 5-second intervals
    qt_date_floor_1 """select date_floor(cast('0001-01-01 00:00:18' as datetime), INTERVAL 5 SECOND)"""
    
    // Datetime with scale, return value also has scale
    qt_date_floor_2 """select date_floor(cast('0001-01-01 00:00:18.123' as datetime), INTERVAL 5 SECOND)"""
    
    // Input time is exactly at the start of a 5-day cycle
    qt_date_floor_3 """select date_floor('2023-07-10 00:00:00', INTERVAL 5 DAY)"""
    
    // Round down date type
    qt_date_floor_4 """select date_floor('2023-07-13', INTERVAL 5 YEAR)"""
    
    // Any parameter is NULL
    qt_date_floor_5 """select date_floor(NULL, INTERVAL 5 HOUR)"""

    validateTimestamptzCeilFloor("select date_floor('2023-03-15 14:25:38.999999+02:00', interval 1 second)", '+02:00')
    validateTimestamptzCeilFloor("select date_floor('2024-06-20 23:59:59.123456-04:00', interval 5 second)", '-04:00')
    validateTimestamptzCeilFloor("select date_floor('2024-06-20 10:01:45+05:00', interval 3 minute)", '+05:00')
    validateTimestamptzCeilFloor("select date_floor('2023-11-09 00:02:30+09:00', interval 5 minute)", '+09:00')
    validateTimestamptzCeilFloor("select date_floor('2023-11-09 01:35:12-05:00', interval 2 hour)", '-05:00')
    validateTimestamptzCeilFloor("select date_floor('2024-03-01 02:45:30Z', interval 3 hour)", '+00:00')
    validateTimestamptzCeilFloor("select date_floor('2024-02-01 20:18:45+01:00', interval 2 day)", '+01:00')
    validateTimestamptzCeilFloor("select date_floor('2024-01-02 22:30:15+05:30', interval 3 day)", '+05:30')
    validateTimestamptzCeilFloor("select date_floor('2023-09-11 23:22:33-07:00', interval 2 week)", '-07:00')
    validateTimestamptzCeilFloor("select date_floor('2025-01-05 20:15:40+03:00', interval 1 week)", '+03:00')
    validateTimestamptzCeilFloor("select date_floor('2024-02-29 22:41:56-06:00', interval 2 month)", '-06:00')
    validateTimestamptzCeilFloor("select date_floor('2024-01-15 21:30:00+02:00', interval 3 month)", '+02:00')
    validateTimestamptzCeilFloor("select date_floor('2025-01-10 20:52:27-08:00', interval 2 year)", '-08:00')
    validateTimestamptzCeilFloor("select date_floor('2024-11-15 22:30:15+01:00', interval 3 year)", '+01:00')

    // 9. DATE_FORMAT function tests
    // Basic formatting tests
    qt_date_format_1 """SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')"""
    qt_date_format_2 """SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s')"""
    qt_date_format_3 """SELECT DATE_FORMAT('1999-01-01', '%Y-%m-%d')"""
    qt_date_format_4 """SELECT DATE_FORMAT('1999-01-01 00:00:00', '%d/%m/%Y %H:%i:%s')"""

    qt_date_format_5 """select date_format('2022-11-13 11:12:12',repeat('%l',51));"""

    test {
        sql """select date_format('2022-11-13 11:12:12',repeat('%l',52));"""
        exception "Operation date_format of 2022-11-13 11:12:12, %l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l is invalid"
    }

    test {
        sql """select date_format('2023-11-13 23:00:00' ,repeat('%l',53));"""
        exception "Operation date_format of 2023-11-13 23:00:00, %l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l%l is invalid"
    }

    test {
        sql """SELECT DATE_FORMAT('1222-12-12', repeat('s',129))"""
        exception  "Operation date_format of invalid or oversized format is invalid"
    }

    test {
        sql """select date_format('2022-11-13',repeat('%I',52));"""
        exception "Operation date_format of 2022-11-13 00:00:00, %I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I%I is invalid"
    }

    test {
        sql """select date_format('2022-11-13',repeat('%M',15));"""
        exception "Operation date_format of 2022-11-13 00:00:00, %M%M%M%M%M%M%M%M%M%M%M%M%M%M%M is invalid"
    }

    test {
        sql """select date_format('2022-11-13',repeat('%p',52));"""
        exception "Operation date_format of 2022-11-13 00:00:00, %p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p%p is invalid"
    }
    
    // Special format specifier tests
    qt_date_format_5 """SELECT DATE_FORMAT('2009-10-04', '%a %b %c')"""
    qt_date_format_6 """SELECT DATE_FORMAT('2009-10-04', '%D %e %f')"""
    
    // Any parameter is NULL
    qt_date_format_7 """SELECT DATE_FORMAT(NULL, '%Y-%m-%d')"""
    qt_date_format_8 """SELECT DATE_FORMAT('2009-10-04', NULL)"""
    qt_date_format_9 """SELECT DATE_FORMAT('2009-10-04 22:23:00', '   %W %M    %Y')"""

    // 10. DATE function tests
    // Extract date part from datetime
    qt_date_1 """SELECT DATE('2003-12-31 01:02:03')"""
    qt_date_2 """SELECT DATE('2003-12-31')"""
    
    // Parameter is NULL
    qt_date_3 """SELECT DATE(NULL)"""

    // 11. DATE_SUB function tests
    // Subtract days
    qt_date_sub_1 """SELECT DATE_SUB('2018-05-01', INTERVAL 1 DAY)"""

    // Subtract months
    qt_date_sub_2 """SELECT DATE_SUB('2018-05-01', INTERVAL 1 MONTH)"""

    // Subtract years
    qt_date_sub_3 """SELECT DATE_SUB('2018-05-01', INTERVAL 1 YEAR)"""

    // Subtract hours
    qt_date_sub_4 """SELECT DATE_SUB('2018-05-01 12:00:00', INTERVAL 2 HOUR)"""

    // Parameter is NULL
    qt_date_sub_5 """SELECT DATE_SUB(NULL, INTERVAL 1 DAY)"""
    qt_date_sub_6 """SELECT DATE_SUB('2023-12-31 23:59:59+08:00', INTERVAL 1 YEAR)"""
    qt_date_sub_7 """SELECT DATE_SUB('2023-06-15 12:30:45-05:00', INTERVAL 2 QUARTER)"""
    qt_date_sub_8 """SELECT DATE_SUB('2023-01-31 10:15:30+00:00', INTERVAL 3 MONTH)"""
    qt_date_sub_9 """SELECT DATE_SUB('2024-02-29 15:45:22+09:00', INTERVAL 1 WEEK)"""
    qt_date_sub_10 """SELECT DATE_SUB('2023-12-25 08:00:00-08:00', INTERVAL 5 DAY)"""
    qt_date_sub_11 """SELECT DATE_SUB('2023-01-01 00:00:00Z', INTERVAL 10 HOUR)"""
    qt_date_sub_12 """SELECT DATE_SUB('2023-06-15 23:59:59+05:30', INTERVAL 45 MINUTE)"""
    qt_date_sub_13 """SELECT DATE_SUB('2023-12-31 23:59:59.999999+08:00', INTERVAL 1 SECOND)"""
    qt_date_sub_14 """SELECT DATE_SUB('2023-03-15 14:30:00+11:00', INTERVAL -1 YEAR)"""
    qt_date_sub_15 """SELECT DATE_SUB('2024-12-31 23:59:59-12:00', INTERVAL -5 DAY)"""
    qt_date_sub_16 """SELECT DATE_SUB('2023-07-04 16:20:15+00:00', INTERVAL -2 HOUR)"""
    qt_date_sub_17 """SELECT DATE_SUB('2025-02-28 18:45:22+06:00', INTERVAL -30 MINUTE)"""
    qt_date_sub_18 """SELECT DATE_SUB('2023-11-11 11:11:11.111111-03:00', INTERVAL -1 SECOND)"""

    // 12. DATE_TRUNC function tests
    // Truncate to year
    qt_date_trunc_1 """SELECT DATE_TRUNC('2019-05-09', 'year')"""
    
    // Truncate to month
    qt_date_trunc_2 """SELECT DATE_TRUNC('2019-05-09', 'month')"""
    
    // Truncate to day
    qt_date_trunc_3 """SELECT DATE_TRUNC('2019-05-09 12:30:45', 'day')"""
    
    // Truncate to hour
    qt_date_trunc_4 """SELECT DATE_TRUNC('2019-05-09 12:30:45', 'hour')"""
    
    // Parameter is NULL
    qt_date_trunc_5 """SELECT DATE_TRUNC(NULL, 'year')"""

    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-03-15 14:25:38.999999+02:00', 'second')", '+02:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-06-20 09:47:59.123456-04:00', 'second')", '-04:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-06-20 09:59:45-04:00', 'minute')", '-04:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-11-08 23:59:30+09:00', 'minute')", '+09:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-11-08 23:35:12+09:00', 'hour')", '+09:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-02-29 23:45:30-05:00', 'hour')", '-05:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-05-15 22:30:15+05:30', 'hour')", '+05:30')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-01-31 23:18:45Z', 'day')", '+00:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-12-31 20:30:15+02:00', 'day')", '+02:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-03-31 22:15:40-06:00', 'day')", '-06:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-09-10 23:22:33+05:30', 'week')", '+05:30')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-12-29 22:15:40-05:00', 'week')", '-05:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-01-31 23:41:56-07:00', 'month')", '-07:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-12-31 20:30:00+01:00', 'month')", '+01:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-06-30 23:45:20+05:00', 'month')", '+05:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-03-31 23:29:14+01:00', 'quarter')", '+01:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-09-30 22:45:30-06:00', 'quarter')", '-06:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-12-31 21:15:45+02:00', 'quarter')", '+02:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2024-12-31 23:52:27-11:00', 'year')", '-11:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2025-12-31 22:30:15+03:00', 'year')", '+03:00')
    validateTimestamptzTrunc("SELECT DATE_TRUNC('2023-12-31 20:15:45+01:00', 'year')", '+01:00')

    // Group 2: Day functions and related date extraction functions
    
    // 13. DAY_CEIL function tests
    qt_day_ceil_1 """select day_ceil( cast('2023-07-13 22:28:18' as datetime), 5)"""
    qt_day_ceil_2 """select day_ceil( '2023-07-13 22:28:18.123', 5)"""
    qt_day_ceil_3 """select day_ceil('2023-07-13 22:28:18')"""
    qt_day_ceil_4 """select day_ceil('2023-07-13 22:28:18', 7, '2023-01-01 00:00:00')"""
    qt_day_ceil_5 """select day_ceil('2023-07-16 00:00:00', 7, '2023-01-01 00:00:00')"""
    qt_day_ceil_6 """select day_ceil(cast('2023-07-13' as date), 3)"""
    // qt_day_ceil_7 """select day_ceil(cast('2023-07-13' as date), 0)"""
    qt_day_ceil_8 """select day_ceil(NULL, 5, '2023-01-01')"""
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18+05:00')", '+05:00')
    validateTimestamptzCeilFloor("select day_ceil('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')", '+00:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18+05:00', 5)", '+05:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18+05:00', 5, '2023-01-01')", '+05:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('0001-01-01 12:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('9999-12-30 23:59:59+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 23:59:59+00:00', 5, '2023-01-01 00:00:00+08:00')", '+00:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18+05:00', 5, '0001-01-01 00:00:00')", '+05:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 22:28:18', 3, '0001-12-31 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('9999-12-25 10:30:45+05:00', 3, '9999-12-01 00:00:00')", '+05:00')
    validateTimestamptzCeilFloor("select day_ceil('9999-12-25 10:30:45', 3, '9999-01-01 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-01-31 23:59:59.999+08:00', 5)", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-02-28 23:59:59.999+08:00', 3, '2023-02-01 00:00:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2024-02-29 23:59:59.999+08:00', 5, '2024-01-01 00:00:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-06-30 23:59:59.999999+08:00', 7)", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-12-31 23:59:59.999+00:00', 5)", '+00:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-12-31 23:59:59.999+08:00', 3, '2023-01-01 00:00:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('0001-12-31 23:59:59.999+08:00', 5)", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 00:00:00.000001+08:00', 5)", '+08:00')
    validateTimestamptzCeilFloor("select day_ceil('2023-07-13 23:59:59.999999+08:00', 5)", '+08:00')

    // 14. DAY_FLOOR function tests
    qt_day_floor_1 """select day_floor('2023-07-13 22:28:18', 5)"""
    qt_day_floor_2 """select day_floor('2023-07-13 22:28:18.123', 5)"""
    qt_day_floor_3 """select day_floor('2023-07-13 22:28:18')"""
    qt_day_floor_4 """select day_floor('2023-07-13 22:28:18', 7, '2023-01-01 00:00:00')"""
    qt_day_floor_5 """select day_floor('2023-07-09 00:00:00', 7, '2023-01-01 00:00:00')"""
    qt_day_floor_6 """select day_floor(cast('2023-07-13' as date), 3)"""
    qt_day_floor_7 """select day_floor(NULL, 5, '2023-01-01')"""
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18+05:00')", '+05:00')
    validateTimestamptzCeilFloor("select day_floor('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')", '+00:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18+05:00', 5)", '+05:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18+05:00', 5, '2023-01-01')", '+05:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('0001-01-02 12:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('9999-12-31 23:59:59+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 23:59:59+00:00', 5, '2023-01-01 00:00:00+08:00')", '+00:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18+05:00', 5, '0001-01-01 00:00:00')", '+05:00')
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 22:28:18', 3, '0001-12-31 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('9999-12-25 10:30:45+05:00', 3, '9999-12-01 00:00:00')", '+05:00')
    validateTimestamptzCeilFloor("select day_floor('9999-12-25 10:30:45', 3, '9999-01-01 00:00:00+08:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2023-01-31 23:59:59.999+08:00', 5)", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2023-02-28 23:59:59.999+08:00', 3, '2023-02-01 00:00:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2024-02-29 23:59:59.999+08:00', 5, '2024-01-01 00:00:00')", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2023-06-30 23:59:59.999999+08:00', 7)", '+08:00')
    validateTimestamptzCeilFloor("select day_floor('2023-12-31 23:59:59.999+00:00', 5)")
    validateTimestamptzCeilFloor("select day_floor('2023-12-31 23:59:59.999+08:00', 3, '2023-01-01 00:00:00')")
    validateTimestamptzCeilFloor("select day_floor('0001-12-31 23:59:59.999+08:00', 5)")
    validateTimestamptzCeilFloor("select day_floor('9999-12-31 12:00:00.123+08:00', 2)")
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 00:00:00.000001+08:00', 5)")
    validateTimestamptzCeilFloor("select day_floor('2023-07-13 23:59:59.999999+08:00', 5)")

    // 15. DAY function tests
    qt_day_1 """select day('1987-01-31')"""
    qt_day_2 """select day('2023-07-13 22:28:18')"""
    qt_day_3 """select day(NULL)"""
    
    // 16. DAYNAME function tests
    qt_dayname_1 """select dayname('2007-02-03 00:00:00')"""
    qt_dayname_2 """select dayname('2023-10-01')"""
    qt_dayname_3 """select dayname(NULL)"""

    sql """SET lc_time_names='ZH_cn'"""
    qt_dayname_zh_cn """SELECT DAYNAME('2023-10-01')"""
    
    sql """SET lc_time_names='zh_TW'"""
    qt_dayname_zh_tw """SELECT DAYNAME('2024-03-15 14:30:25')"""
    
    sql """SET lc_time_names='zh_HK'"""
    qt_dayname_zh_hk """SELECT DAYNAME('2022-12-25')"""
    
    sql """SET lc_time_names='fr_FR'"""
    qt_dayname_fr_fr """SELECT DAYNAME('2023-07-14 09:15:30')"""
    
    sql """SET lc_time_names='fr_BE'"""
    qt_dayname_fr_be """SELECT DAYNAME('2024-01-01')"""
    
    sql """SET lc_time_names='fr_CA'"""
    qt_dayname_fr_ca """SELECT DAYNAME('2023-11-11 23:59:59')"""
    
    sql """SET lc_time_names='de_DE'"""
    qt_dayname_de_de """SELECT DAYNAME('2024-02-29')"""
    
    sql """SET lc_time_names='DE_at'"""
    qt_dayname_de_at """SELECT DAYNAME('2023-05-20 06:45:12')"""
    
    sql """SET lc_time_names='de_CH'"""
    qt_dayname_de_ch """SELECT DAYNAME('2024-08-31')"""
    
    sql """SET lc_time_names='ja_JP'"""
    qt_dayname_ja_jp """SELECT DAYNAME('2023-04-29 12:00:00')"""
    
    sql """SET lc_time_names='ko_KR'"""
    qt_dayname_ko_kr """SELECT DAYNAME('2024-09-15')"""
    
    sql """SET lc_time_names='ES_es'"""
    qt_dayname_es_es """SELECT DAYNAME('2023-12-31 18:30:45')"""
    
    sql """SET lc_time_names='es_MX'"""
    qt_dayname_es_mx """SELECT DAYNAME('2024-05-05')"""
    
    sql """SET lc_time_names='es_AR'"""
    qt_dayname_es_ar """SELECT DAYNAME('2023-06-21 03:15:22')"""
    
    sql """SET lc_time_names='RU_ru'"""
    qt_dayname_ru_ru """SELECT DAYNAME('2024-07-04')"""
    
    sql """SET lc_time_names='ru_UA'"""
    qt_dayname_ru_ua """SELECT DAYNAME('2023-02-14 16:45:33')"""
    
    sql """SET lc_time_names='it_IT'"""
    qt_dayname_it_it """SELECT DAYNAME('2024-10-12')"""
    
    sql """SET lc_time_names='IT_ch'"""
    qt_dayname_it_ch """SELECT DAYNAME('2023-03-08 21:20:15')"""
    
    sql """SET lc_time_names='ar_SA'"""
    qt_dayname_ar_sa """SELECT DAYNAME('2024-04-15')"""
    
    sql """SET lc_time_names='ar_AE'"""
    qt_dayname_ar_ae """SELECT DAYNAME('2023-08-25 11:30:40')"""
    
    sql """SET lc_time_names='AR_eg'"""
    qt_dayname_ar_eg """SELECT DAYNAME('2024-01-20')"""
    
    sql """SET lc_time_names='en_US'"""
    qt_dayname_en_us """SELECT DAYNAME('2023-09-11 07:45:55')"""
    
    sql """SET lc_time_names='en_GB'"""
    qt_dayname_en_gb """SELECT DAYNAME('2024-06-30')"""
    
    sql """SET lc_time_names='En_Au'"""
    qt_dayname_en_au """SELECT DAYNAME('2023-01-26 19:25:10')"""
    
    sql """SET lc_time_names='en_CA'"""
    qt_dayname_en_ca """SELECT DAYNAME('2024-11-11')"""
    
    sql """SET lc_time_names='pt_BR'"""
    qt_dayname_pt_br """SELECT DAYNAME('2023-09-07 14:15:28')"""
    
    sql """SET lc_time_names='pt_PT'"""
    qt_dayname_pt_pt """SELECT DAYNAME('2024-12-08')"""
    
    sql """SET lc_time_names='nl_NL'"""
    qt_dayname_nl_nl """SELECT DAYNAME('2023-04-27 10:30:45')"""
    
    sql """SET lc_time_names='NL_be'"""
    qt_dayname_nl_be """SELECT DAYNAME('2024-03-21')"""
    
    sql """SET lc_time_names='sv_SE'"""
    qt_dayname_sv_se """SELECT DAYNAME('2023-06-06 22:15:35')"""
    
    sql """SET lc_time_names='no_NO'"""
    qt_dayname_no_no """SELECT DAYNAME('2024-05-17')"""
    
    sql """SET lc_time_names='da_DK'"""
    qt_dayname_da_dk """SELECT DAYNAME('2023-12-03 08:20:17')"""
    
    sql """SET lc_time_names='fi_FI'"""
    qt_dayname_fi_fi """SELECT DAYNAME('2024-01-15')"""
    
    sql """SET lc_time_names='pl_PL'"""
    qt_dayname_pl_pl """SELECT DAYNAME('2023-11-30 13:45:22')"""
    
    sql """SET lc_time_names='cs_CZ'"""
    qt_dayname_cs_cz """SELECT DAYNAME('2024-06-20')"""
    
    sql """SET lc_time_names='hu_HU'"""
    qt_dayname_hu_hu """SELECT DAYNAME('2023-08-15 20:10:33')"""
    
    sql """SET lc_time_names='th_TH'"""
    qt_dayname_th_th """SELECT DAYNAME('2024-09-22')"""
    
    sql """SET lc_time_names='vi_VN'"""
    qt_dayname_vi_vn """SELECT DAYNAME('2023-05-01 15:35:44')"""
    
    sql """SET lc_time_names='tr_TR'"""
    qt_dayname_tr_tr """SELECT DAYNAME('2024-02-14')"""
    
    sql """SET lc_time_names='el_GR'"""
    qt_dayname_el_gr """SELECT DAYNAME('2023-10-28 04:25:56')"""
    
    sql """SET lc_time_names='he_IL'"""
    qt_dayname_he_il """SELECT DAYNAME('2024-04-07')"""
    
    sql """SET lc_time_names='hi_IN'"""
    qt_dayname_hi_in """SELECT DAYNAME('2023-07-20 17:55:11')"""
    
    sql """SET lc_time_names='id_ID'"""
    qt_dayname_id_id """SELECT DAYNAME('2024-11-05')"""
    
    sql """SET lc_time_names='ms_MY'"""
    qt_dayname_ms_my """SELECT DAYNAME('2023-03-18 12:40:28')"""

    sql """SET lc_time_names='ar_AE'"""
    testFoldConst("SELECT DAYNAME('2023-08-15 20:10:33');")
    
    sql """SET lc_time_names='zh_CN'"""
    testFoldConst("SELECT DAYNAME('2024-09-22');")
    
    sql """SET lc_time_names='ja_JP'"""
    testFoldConst("SELECT DAYNAME('2023-05-01 15:35:44');")
    
    sql """SET lc_time_names='ko_KR'"""
    testFoldConst("SELECT DAYNAME('2024-02-14');")
    
    sql """SET lc_time_names='ru_RU'"""
    testFoldConst("SELECT DAYNAME('2023-10-28 04:25:56');")
    
    sql """SET lc_time_names='de_DE'"""
    testFoldConst("SELECT DAYNAME('2024-04-07');")
    
    sql """SET lc_time_names='fr_FR'"""
    testFoldConst("SELECT DAYNAME('2023-07-20 17:55:11');")
    
    sql """SET lc_time_names='es_ES'"""
    testFoldConst("SELECT DAYNAME('2024-11-05');")
    
    sql """SET lc_time_names='pt_BR'"""
    testFoldConst("SELECT DAYNAME('2023-03-18 12:40:28');")
    
    sql """SET lc_time_names='it_IT'"""
    testFoldConst("SELECT DAYNAME('2024-01-01');")
    
    sql """SET lc_time_names='nl_NL'"""
    testFoldConst("SELECT DAYNAME('2023-12-31');")
    
    sql """SET lc_time_names='sv_SE'"""
    testFoldConst("SELECT DAYNAME('2024-06-30 23:59:59');")
    
    sql """SET lc_time_names='pl_PL'"""
    testFoldConst("SELECT DAYNAME('2023-02-28');")
    
    sql """SET lc_time_names='cs_CZ'"""
    testFoldConst("SELECT DAYNAME('2024-02-29');")
    
    sql """SET lc_time_names='bg_BG'"""
    testFoldConst("SELECT DAYNAME('1970-01-01');")
    
    sql """SET lc_time_names='uk_UA'"""
    testFoldConst("SELECT DAYNAME('2038-01-19');")
    
    sql """SET lc_time_names='en_US'"""
    testFoldConst("SELECT DAYNAME(NULL);")

    sql """SET lc_time_names=default"""
    
    // 17. DAYOFWEEK function tests
    qt_dayofweek_1 """select dayofweek('2019-06-25')"""
    qt_dayofweek_2 """select dayofweek('2019-06-25 15:30:45')"""
    qt_dayofweek_3 """select dayofweek('2024-02-18')"""
    qt_dayofweek_4 """select dayofweek(NULL)"""
    
    // 18. DAYOFYEAR function tests
    qt_dayofyear_1 """select dayofyear('2007-02-03 00:00:00')"""
    qt_dayofyear_2 """select dayofyear('2023-12-31')"""
    qt_dayofyear_3 """select dayofyear('2024-12-31')"""
    qt_dayofyear_4 """select dayofyear(NULL)"""
    
    // 19. EXTRACT function tests
    qt_extract_1 """select extract(year from '2022-09-22 17:01:30') as year, extract(month from '2022-09-22 17:01:30') as month, extract(day from '2022-09-22 17:01:30') as day, extract(hour from '2022-09-22 17:01:30') as hour, extract(minute from '2022-09-22 17:01:30') as minute, extract(second from '2022-09-22 17:01:30') as second, extract(microsecond from cast('2022-09-22 17:01:30.000123' as datetime(6))) as microsecond"""
    qt_extract_2 """select extract(quarter from '2023-05-15') as quarter"""
    qt_extract_3 """select extract(week from '2024-01-06') as week"""
    qt_extract_4 """select extract(week from '2024-01-07') as week"""
    qt_extract_5 """select extract(week from '2024-12-31') as week"""

    // 20. FROM_DAYS function tests
    qt_from_days_1 """select from_days(730669),from_days(5),from_days(59), from_days(60)"""
    // qt_from_days_2 """select from_days(-60)"""
    qt_from_days_3 """select from_days(NULL)"""

    // 21. FROM_ISO8601_DATE function tests
    qt_from_iso8601_date_1 """select from_iso8601_date('2023') as year_only, from_iso8601_date('2023-10') as year_and_month, from_iso8601_date('2023-10-05') as full_date"""
    qt_from_iso8601_date_2 """select from_iso8601_date('2021-001') as day_1, from_iso8601_date('2021-059') as day_59, from_iso8601_date('2021-060') as day_60, from_iso8601_date('2024-366') as day_366"""
    qt_from_iso8601_date_3 """select from_iso8601_date('0522-W01-1') as week_1"""
    qt_from_iso8601_date_4 """select from_iso8601_date('0522-W01-4') as week_4"""
    qt_from_iso8601_date_5 """select from_iso8601_date('0522-W01') as week_1"""
    qt_from_iso8601_date_6 """select from_iso8601_date('2023-10-01T12:34:10')"""
    qt_from_iso8601_date_7 """select from_iso8601_date('0522-W61') as week_61"""
    qt_from_iso8601_date_8 """select from_iso8601_date('0522-661') as day_661"""
    qt_from_iso8601_date_9 """select from_iso8601_date(NULL)"""
            sql """select from_iso8601_date('2023-10-01T12:34:10');"""
    
    // 22. FROM_MICROSECOND function tests
    qt_from_microsecond_1 """SELECT FROM_MICROSECOND(0)"""
    qt_from_microsecond_2 """SELECT FROM_MICROSECOND(1700000000000000)"""
    qt_from_microsecond_3 """select from_microsecond(1700000000123456) as dt_with_micro"""
    qt_from_microsecond_4 """select from_microsecond(NULL)"""
    
    // 23. FROM_MILLISECOND function tests
    qt_from_millisecond_1 """SELECT FROM_MILLISECOND(0)"""
    qt_from_millisecond_2 """SELECT FROM_MILLISECOND(1700000000000)"""
    qt_from_millisecond_3 """select from_millisecond(1700000000123) as dt_with_milli"""
    qt_from_millisecond_4 """select from_millisecond(NULL)"""
    
    // 24. FROM_SECOND function tests
    qt_from_second_1 """SELECT FROM_SECOND(0)"""
    qt_from_second_2 """SELECT FROM_SECOND(1700000000)"""
    qt_from_second_3 """select from_second(NULL)"""
    
    // 25. FROM_UNIXTIME function tests
    qt_from_unixtime_1 """select from_unixtime(0)"""
    qt_from_unixtime_2 """select from_unixtime(1196440219)"""
    qt_from_unixtime_3 """select from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss')"""
    qt_from_unixtime_4 """select from_unixtime(1196440219, '%Y-%m-%d')"""
    qt_from_unixtime_5 """select from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s')"""
    qt_from_unixtime_6 """select from_unixtime(32536799,'gdaskpdp')"""
    qt_from_unixtime_7 """select from_unixtime(NULL)"""

    // Group 3: Hour and time manipulation functions (序号 26-43)
    
    // 26. HOUR_CEIL function tests
    qt_hour_ceil_1 """select hour_ceil("2023-07-13 22:28:18", 5)"""
    qt_hour_ceil_2 """select hour_ceil('2023-07-13 19:30:00', 4, '2023-07-13 08:00:00')"""
    qt_hour_ceil_3 """select hour_ceil('2023-07-13 00:30:00', 6, '2023-07-13')"""
    qt_hour_ceil_4 """select hour_ceil('2023-07-13 01:00:00')"""
    qt_hour_ceil_5 """select hour_ceil('2023-07-13 19:30:00', 4, '2023-07-13 08:00:00.123')"""
    qt_hour_ceil_6 """select hour_ceil('2023-07-13 19:30:00.123', 4, '2023-07-13 08:00:00')"""
    qt_hour_ceil_7 """select hour_ceil(null, 3)"""
    qt_hour_ceil_8 """select hour_ceil("2023-07-13 22:28:18", NULL)"""
    qt_hour_ceil_9 """select hour_ceil("2023-07-13 22:28:18", 5, NULL)"""
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 22:28:18.456789+05:00', 4)", '+12:34')
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 22:28:18.456789+05:00', 4, '2023-07-13 08:00:00')", '-07:13')
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 22:28:18.789123', 5, '2023-07-13 18:00:00+08:00')", '+00:01')
    validateTimestamptzCeilFloor("select hour_ceil('0001-07-13 22:28:18.456789+05:00', 4)", '-00:01')
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 23:59:59.999+00:00', 4, '2023-07-13 20:00:00+08:00')", '+13:59')
    validateTimestamptzCeilFloor("select hour_ceil('2023-12-31 23:30:00.111+00:00', 5)", '-11:22')
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 22:28:18+05:00', 6, '0001-01-01 00:00:00')", '+05:45')
    validateTimestamptzCeilFloor("select hour_ceil('9999-12-31 10:30:45+05:00', 6, '9999-12-31 00:00:00')", '-03:30')
    validateTimestamptzCeilFloor("select hour_ceil('2023-01-01 23:59:59.999+08:00', 6)", '+04:30')
    validateTimestamptzCeilFloor("select hour_ceil('2023-06-30 23:59:59.999+08:00', 6, '2023-06-30 00:00:00')", '-02:30')
    validateTimestamptzCeilFloor("select hour_ceil('2023-12-31 23:59:59.999+00:00', 6)", '+08:45')
    validateTimestamptzCeilFloor("select hour_ceil('0001-01-01 23:59:59.999+08:00', 4)", '+00:13')
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 00:00:00.000001+08:00', 6)", '-00:17')
    validateTimestamptzCeilFloor("select hour_ceil('2023-07-13 23:59:59.999999+08:00', 6)", '+11:11')

    // 27. HOUR_FLOOR function tests
    qt_hour_floor_1 """select hour_floor("2023-07-13 22:28:18", 5)"""
    qt_hour_floor_2 """select hour_floor('2023-07-13 19:30:00', 4, '2023-07-13 08:00:00')"""
    qt_hour_floor_3 """select hour_floor("2023-07-13 18:00:00", 5)"""
    qt_hour_floor_4 """select hour_floor('2023-07-13 20:30:00', 4, '2023-07-13')"""
    qt_hour_floor_5 """select hour_floor('2023-07-13 19:30:00.123', 4, '2023-07-03 08:00:00')"""
    qt_hour_floor_6 """select hour_floor('2023-07-13 19:30:00', 4, '2023-07-03 08:00:00.123')"""
    qt_hour_floor_7 """select hour_floor(null, 6)"""
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 22:28:18.456789+05:00', 4)", '-11:11')
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 22:28:18.456789+05:00', 4, '2023-07-13 08:00:00')", '+09:30')
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 22:28:18.789123', 5, '2023-07-13 18:00:00+08:00')", '-09:30')
    validateTimestamptzCeilFloor("select hour_floor('0001-07-13 22:28:18.456789+05:00', 4)", '+06:30')
    validateTimestamptzCeilFloor("select hour_floor('9999-12-31 22:28:18.456789+05:00', 4)", '-04:30')
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 23:59:59.999+00:00', 4, '2023-07-13 20:00:00+08:00')", '+03:30')
    validateTimestamptzCeilFloor("select hour_floor('2023-12-31 23:30:00.111+00:00', 5)", '-03:30')
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 22:28:18+05:00', 6, '0001-01-01 00:00:00')", '+10:30')
    validateTimestamptzCeilFloor("select hour_floor('9999-12-31 10:30:45+05:00', 6, '9999-12-31 00:00:00')", '-10:30')
    validateTimestamptzCeilFloor("select hour_floor('2023-01-01 23:59:59.999+08:00', 6)", '+12:45')
    validateTimestamptzCeilFloor("select hour_floor('2023-06-30 23:59:59.999+08:00', 6, '2023-06-30 00:00:00')", '-00:45')
    validateTimestamptzCeilFloor("select hour_floor('2023-12-31 23:59:59.999+00:00', 6)", '+05:30')
    validateTimestamptzCeilFloor("select hour_floor('0001-01-01 23:59:59.999+08:00', 4)", '-05:30')
    validateTimestamptzCeilFloor("select hour_floor('9999-12-31 23:00:00.123+08:00', 4)", '+08:00')
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 00:00:00.000001+08:00', 6)", '-08:00')
    validateTimestamptzCeilFloor("select hour_floor('2023-07-13 23:59:59.999999+08:00', 6)", '+01:00')

    // 28. HOUR function tests
    qt_hour_1 """select hour('2018-12-31 23:59:59'), hour('2023-01-01 00:00:00'), hour('2023-10-01 12:30:45')"""
    qt_hour_2 """select hour(cast('14:30:00' as time)), hour(cast('25:00:00' as time)), hour(cast('456:26:32' as time))"""
    qt_hour_3 """select hour(cast('-12:30:00' as time)), hour(cast('838:59:59' as time)), hour(cast('-838:59:59' as time))"""
    qt_hour_4 """select hour('2023-10-01')"""
    qt_hour_5 """select hour(NULL)"""
    
    // 29. HOURS_ADD function tests
    qt_hours_add_1 """SELECT HOURS_ADD('2020-02-02 02:02:02', 1)"""
    qt_hours_add_2 """SELECT HOURS_ADD('2020-02-02', 51)"""
    qt_hours_add_3 """select hours_add('2023-10-01 10:00:00', -3)"""
    qt_hours_add_4 """select hours_add(null, 5)"""
    qt_hours_add_5 """select hours_add('2023-10-01 10:00:00', NULL)"""
    qt_hours_add_6 """SELECT HOURS_ADD('2023-12-31 22:00:00+08:00', 3)"""
    qt_hours_add_7 """SELECT HOURS_ADD('2023-01-01 00:00:00Z', 24)"""
    qt_hours_add_8 """SELECT HOURS_ADD('2024-02-29 12:30:45.123-05:00', 12)"""
    qt_hours_add_9 """SELECT HOURS_ADD('0001-01-01 00:00:00+00:00', 48)"""
    qt_hours_add_10 """SELECT HOURS_ADD('2023-06-15 15:45:30.555+09:00', -6)"""
    qt_hours_add_11 """SELECT HOURS_ADD('9999-12-31 23:00:00-12:00', 1)"""
    qt_hours_add_12 """SELECT HOURS_ADD('2023-03-15 10:20:30+05:30', -18)"""
    
    // 30. HOURS_DIFF function tests
    qt_hours_diff_1 """select hours_diff('2020-12-25 22:00:00', '2020-12-25 08:00:00')"""
    qt_hours_diff_2 """select hours_diff('2023-06-15 10:30:00', '2023-06-15 14:45:00')"""
    qt_hours_diff_3 """select hours_diff('2020-03-01', '2020-02-28')"""
    qt_hours_diff_4 """select hours_diff('2024-02-29', '2024-02-28')"""
    qt_hours_diff_5 """select hours_diff('2023-10-01 10:00:00', NULL)"""
    qt_hours_diff_6 """select hours_diff(NULL, '2023-10-01 10:00:00')"""
    
    // 31. HOURS_SUB function tests
    qt_hours_sub_1 """select hours_sub('2020-02-02 02:02:02', 1)"""
    qt_hours_sub_2 """select hours_sub('2020-02-02', 10)"""
    qt_hours_sub_3 """select hours_sub('2023-10-01 10:00:00', -5)"""
    qt_hours_sub_4 """select hours_sub(null, 3)"""
    qt_hours_sub_5 """select hours_sub('2023-10-01 10:00:00', NULL)"""
    qt_hours_sub_6 """SELECT HOURS_SUB('2023-12-31 22:00:00+08:00', 3)"""
    qt_hours_sub_7 """SELECT HOURS_SUB('2023-01-01 00:00:00Z', 24)"""
    qt_hours_sub_8 """SELECT HOURS_SUB('2024-02-29 12:30:45.123-05:00', 12)"""
    qt_hours_sub_9 """SELECT HOURS_SUB('0001-01-01 00:00:00+00:00', 48)"""
    qt_hours_sub_10 """SELECT HOURS_SUB('2023-06-15 15:45:30.555+09:00', -6)"""
    qt_hours_sub_11 """SELECT HOURS_SUB('9999-12-31 23:00:00-12:00', 1)"""
    qt_hours_sub_12 """SELECT HOURS_SUB('2023-03-15 10:20:30+05:30', -18)"""
    
    // 32. LAST_DAY function tests
    qt_last_day_1 """SELECT LAST_DAY('2000-02-03')"""
    qt_last_day_2 """SELECT LAST_DAY('2023-04-15 12:34:56')"""
    qt_last_day_3 """SELECT LAST_DAY('2023-02-28')"""
    qt_last_day_4 """SELECT LAST_DAY('2024-02-15')"""
    qt_last_day_5 """SELECT LAST_DAY('2023-12-25')"""
    qt_last_day_6 """SELECT LAST_DAY('2023-01-01')"""
    qt_last_day_7 """SELECT LAST_DAY(NULL)"""
    
    
    // 34. MAKEDATE function tests
    qt_makedate_1 """select makedate(2021, 1), makedate(2021, 32), makedate(2021, 365)"""
    qt_makedate_2 """select makedate(2024, 1), makedate(2024, 60), makedate(2024, 366)"""
    // qt_makedate_3 """select makedate(2021, 0)"""
    qt_makedate_4 """select makedate(2021, 400)"""
    qt_makedate_5 """select makedate(NULL, 100)"""
    qt_makedate_6 """select makedate(2021, NULL)"""
    
    // 35. MICROSECOND function tests
    qt_microsecond_1 """select microsecond('2019-01-01 00:00:00.123456')"""
    qt_microsecond_2 """select microsecond(cast('14:30:25.123456' as time))"""
    qt_microsecond_3 """select microsecond('2019-01-01 00:00:00')"""
    qt_microsecond_4 """select microsecond('2019-01-01')"""
    qt_microsecond_5 """select microsecond(NULL)"""

    // 36. MICROSECONDS_ADD function tests
    qt_microseconds_add_1 """select microseconds_add('2020-02-02 02:02:02', 1000000)"""
    qt_microseconds_add_2 """select microseconds_add('2023-10-01 12:30:45.123456', 500000)"""
    qt_microseconds_add_3 """select microseconds_add('2020-02-02', 2000000)"""
    qt_microseconds_add_4 """select microseconds_add('2023-10-01 10:00:00', -1000000)"""
    qt_microseconds_add_5 """select microseconds_add(null, 1000000)"""
    qt_microseconds_add_6 """select microseconds_add('2023-10-01 10:00:00', NULL)"""
    qt_microseconds_add_7 """select microseconds_add('2023-12-31 23:59:59.999999+08:00', 1000000)"""
    qt_microseconds_add_8 """select microseconds_add('2023-01-01 00:00:00Z', 500000)"""
    qt_microseconds_add_9 """select microseconds_add('2024-02-29 12:30:45.123456-05:00', 2000000)"""
    qt_microseconds_add_10 """select microseconds_add('0001-01-01 00:00:00+00:00', 999999)"""
    qt_microseconds_add_11 """select microseconds_add('2023-06-15 15:45:30.555555+09:00', -500000)"""
    qt_microseconds_add_12 """select microseconds_add('9999-12-31 23:59:59.123456-12:00', 1000000)"""
    qt_microseconds_add_13 """select microseconds_add('2023-03-15 10:20:30+05:30', -1000000)"""
    qt_microseconds_add_14 """select microseconds_add('2025-07-04 16:20:15.777777-07:00', NULL)"""

    // 37. MICROSECONDS_DIFF function tests
    qt_microseconds_diff_1 """select microseconds_diff('2020-12-25 22:00:00.123456', '2020-12-25 22:00:00.000000')"""
    qt_microseconds_diff_2 """select microseconds_diff('2023-06-15 10:30:00', '2023-06-15 10:29:59')"""
    qt_microseconds_diff_3 """select microseconds_diff('2020-03-01 00:00:00.500000', '2020-02-29 23:59:59.000000')"""
    qt_microseconds_diff_4 """select microseconds_diff('2023-10-01 10:00:00', NULL)"""
    qt_microseconds_diff_5 """select microseconds_diff(NULL, '2023-10-01 10:00:00')"""

    // 38. MICROSECONDS_SUB function tests
    qt_microseconds_sub_1 """select microseconds_sub('2020-02-02 02:02:02.123456', 500000)"""
    qt_microseconds_sub_2 """select microseconds_sub('2023-10-01 12:30:45', 1000000)"""
    qt_microseconds_sub_3 """select microseconds_sub('2020-02-02', 1000000)"""
    qt_microseconds_sub_4 """select microseconds_sub('2023-10-01 10:00:00', -500000)"""
    qt_microseconds_sub_5 """select microseconds_sub(null, 1000000)"""
    qt_microseconds_sub_6 """select microseconds_sub('2023-10-01 10:00:00', NULL)"""
    qt_microseconds_sub_7 """select microseconds_sub('2023-12-31 23:59:59.999999+08:00', 500000)"""
    qt_microseconds_sub_8 """select microseconds_sub('2023-01-01 00:00:00Z', 1000000)"""
    qt_microseconds_sub_9 """select microseconds_sub('2024-02-29 12:30:45.123456-05:00', 1000000)"""
    qt_microseconds_sub_10 """select microseconds_sub('0001-01-01 00:00:00+00:00', 999999)"""
    qt_microseconds_sub_11 """select microseconds_sub('2023-06-15 15:45:30.555555+09:00', -500000)"""
    qt_microseconds_sub_12 """select microseconds_sub('9999-12-31 23:59:59.123456-12:00', 2000000)"""
    qt_microseconds_sub_13 """select microseconds_sub('2023-03-15 10:20:30+05:30', -1000000)"""
    qt_microseconds_sub_14 """select microseconds_sub('2025-07-04 16:20:15.777777-07:00', NULL)"""
    
    // 39. MICROSECOND_TIMESTAMP function tests
    qt_microsecond_timestamp_1 """SELECT MICROSECOND_TIMESTAMP('2025-01-23 12:34:56.123456')"""
    qt_microsecond_timestamp_2 """SELECT MICROSECOND_TIMESTAMP('2025-01-23 12:34:56.123456 UTC')"""
    qt_microsecond_timestamp_3 """SELECT MICROSECOND_TIMESTAMP('1970-01-01')"""
    qt_microsecond_timestamp_4 """SELECT MICROSECOND_TIMESTAMP('1960-01-01 00:00:00  UTC')"""
    qt_microsecond_timestamp_5 """SELECT MICROSECOND_TIMESTAMP(NULL)"""

    // 40. MILLISECONDS_ADD function tests
    qt_milliseconds_add_1 """select milliseconds_add('2020-02-02 02:02:02', 1000)"""
    qt_milliseconds_add_2 """select milliseconds_add('2023-10-01 12:30:45.123', 500)"""
    qt_milliseconds_add_3 """select milliseconds_add('2020-02-02', 2000)"""
    qt_milliseconds_add_4 """select milliseconds_add('2023-10-01 10:00:00', -1000)"""
    qt_milliseconds_add_5 """select milliseconds_add(null, 1000)"""
    qt_milliseconds_add_6 """select milliseconds_add('2023-10-01 10:00:00', NULL)"""
    qt_milliseconds_add_7 """select milliseconds_add('2023-12-31 23:59:59.999+08:00', 1000)"""
    qt_milliseconds_add_8 """select milliseconds_add('2023-01-01 00:00:00Z', 500)"""
    qt_milliseconds_add_9 """select milliseconds_add('2024-02-29 12:30:45.123-05:00', 2000)"""
    qt_milliseconds_add_10 """select milliseconds_add('0001-01-01 00:00:00+00:00', 999)"""
    qt_milliseconds_add_11 """select milliseconds_add('2023-06-15 15:45:30.555+09:00', -500)"""
    qt_milliseconds_add_12 """select milliseconds_add('9999-12-31 23:59:59.123-12:00', 1000)"""
    qt_milliseconds_add_13 """select milliseconds_add('2023-03-15 10:20:30+05:30', -1000)"""
    qt_milliseconds_add_14 """select milliseconds_add('2025-07-04 16:20:15.777-07:00', NULL)"""

    // 41. MILLISECONDS_DIFF function tests
    qt_milliseconds_diff_1 """select milliseconds_diff('2020-12-25 22:00:00.123', '2020-12-25 22:00:00.000')"""
    qt_milliseconds_diff_2 """select milliseconds_diff('2023-06-15 10:30:00', '2023-06-15 10:29:59')"""
    qt_milliseconds_diff_3 """select milliseconds_diff('2020-03-01 00:00:00.500', '2020-02-29 23:59:59.000')"""
    qt_milliseconds_diff_4 """select milliseconds_diff('2023-10-01 10:00:00', NULL)"""
    qt_milliseconds_diff_5 """select milliseconds_diff(NULL, '2023-10-01 10:00:00')"""

    // 42. MILLISECONDS_SUB function tests
    qt_milliseconds_sub_1 """select milliseconds_sub('2020-02-02 02:02:02.123', 500)"""
    qt_milliseconds_sub_2 """select milliseconds_sub('2023-10-01 12:30:45', 1000)"""
    qt_milliseconds_sub_3 """select milliseconds_sub('2020-02-02', 1000)"""
    qt_milliseconds_sub_4 """select milliseconds_sub('2023-10-01 10:00:00', -500)"""
    qt_milliseconds_sub_5 """select milliseconds_sub(null, 1000)"""
    qt_milliseconds_sub_6 """select milliseconds_sub('2023-10-01 10:00:00', NULL)"""
    qt_milliseconds_sub_7 """select milliseconds_sub('2023-12-31 23:59:59.999+08:00', 500)"""
    qt_milliseconds_sub_8 """select milliseconds_sub('2023-01-01 00:00:00Z', 1000)"""
    qt_milliseconds_sub_9 """select milliseconds_sub('2024-02-29 12:30:45.123-05:00', 1000)"""
    qt_milliseconds_sub_10 """select milliseconds_sub('0001-01-01 00:00:00+00:00', 999)"""
    qt_milliseconds_sub_11 """select milliseconds_sub('2023-06-15 15:45:30.555+09:00', -500)"""
    qt_milliseconds_sub_12 """select milliseconds_sub('9999-12-31 23:59:59.123-12:00', 2000)"""
    qt_milliseconds_sub_13 """select milliseconds_sub('2023-03-15 10:20:30+05:30', -1000)"""
    qt_milliseconds_sub_14 """select milliseconds_sub('2025-07-04 16:20:15.777-07:00', NULL)"""
    
    // 43. MILLISECOND_TIMESTAMP function tests
    qt_millisecond_timestamp_1 """SELECT MILLISECOND_TIMESTAMP('2025-01-23 12:34:56.123')"""
    qt_millisecond_timestamp_2 """SELECT MILLISECOND_TIMESTAMP('2025-01-23 12:34:56.123 UTC')"""
    qt_millisecond_timestamp_3 """SELECT MILLISECOND_TIMESTAMP('2024-01-01 00:00:00.123456')"""
    qt_millisecond_timestamp_4 """SELECT MILLISECOND_TIMESTAMP('1960-01-01 00:00:00  UTC')"""
    qt_millisecond_timestamp_5 """SELECT MILLISECOND_TIMESTAMP('1970-01-01')"""
    qt_millisecond_timestamp_6 """SELECT MILLISECOND_TIMESTAMP(NULL)"""

    // Group 4: Minute and Month functions (序号 44-62)
    
    // 44. MINUTE_CEIL function tests
    qt_minute_ceil_1 """SELECT MINUTE_CEIL('2023-07-13 22:28:18')"""
    qt_minute_ceil_2 """SELECT MINUTE_CEIL('2023-07-13 22:28:18.123',5)"""
    qt_minute_ceil_3 """SELECT MINUTE_CEIL('2023-07-13 22:30:00',5)"""
    qt_minute_ceil_4 """SELECT MINUTE_CEIL('2023-07-13 22:28:18', 5, '2023-07-13 22:20:00')"""
    qt_minute_ceil_5 """SELECT MINUTE_CEIL('2023-07-13 22:28:18.456789', 5)"""
    qt_minute_ceil_6 """SELECT MINUTE_CEIL('2023-07-13', 30)"""
    qt_minute_ceil_7 """SELECT MINUTE_CEIL(NULL, 5), MINUTE_CEIL('2023-07-13 22:28:18', NULL)"""
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.999999+03:00')", '+12:34')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.000001+08:00')", '-09:45')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.500000+05:00', 5)", '+00:00')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.456789+05:00', '2023-07-13 22:20:00')", '-11:11')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.789123', '2023-07-13 22:20:00+08:00')", '+05:30')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.123456+05:00', 5, '2023-07-13 22:20:00')", '-03:00')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:18.654321', 5, '2023-07-13 22:20:00+08:00')", '+08:45')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 23:59:59.999+00:00')", '-06:00')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 23:59:59.111+00:00', '2023-07-13 23:50:00+08:00')", '+01:00')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 22:28:30.678-01:00', 15)", '-07:30')
    validateTimestamptzCeilFloor("SELECT MINUTE_CEIL('2023-07-13 23:59:59.999+00:00', 10, '2023-07-13 23:50:00+08:00')", '+10:00')

    // 45. MINUTE_FLOOR function tests
    qt_minute_floor_1 """SELECT MINUTE_FLOOR('2023-07-13 22:28:18')"""
    qt_minute_floor_2 """SELECT MINUTE_FLOOR('2023-07-13 22:28:18.123', 5)"""
    qt_minute_floor_3 """SELECT MINUTE_FLOOR('2023-07-13 22:25:00', 5)"""
    qt_minute_floor_4 """SELECT MINUTE_FLOOR('2023-07-13 22:28:18', 5, '2023-07-13 22:20:00')"""
    qt_minute_floor_5 """SELECT MINUTE_FLOOR('2023-07-13 22:28:18.456789', 5)"""
    qt_minute_floor_6 """SELECT MINUTE_FLOOR('2023-07-13', 30)"""
    qt_minute_floor_7 """SELECT MINUTE_FLOOR(NULL, 5), MINUTE_FLOOR('2023-07-13 22:28:18', NULL)"""
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.999999+03:00')", '+13:00')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.000001+08:00')", '-10:30')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.500000+05:00', 5)", '+02:15')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.456789+05:00', '2023-07-13 22:20:00')", '-05:45')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.789123', '2023-07-13 22:20:00+08:00')", '+06:30')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.123456+05:00', 5, '2023-07-13 22:20:00')", '-04:15')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.654321', 5, '2023-07-13 22:20:00+08:00')", '+09:15')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 23:59:59.999+00:00')", '-08:45')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 23:59:59.111+00:00', '2023-07-13 23:50:00+08:00')", '+04:00')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 22:28:30.678-01:00', 15)", '-02:30')
    validateTimestamptzCeilFloor("SELECT MINUTE_FLOOR('2023-07-13 23:59:59.999+00:00', 10, '2023-07-13 23:50:00+08:00')", '+11:30')

    // 46. MINUTE function tests
    qt_minute_1 """SELECT MINUTE('2018-12-31 23:59:59')"""
    qt_minute_2 """SELECT MINUTE('2023-05-01 10:05:30.123456')"""
    qt_minute_3 """SELECT MINUTE('14:25:45')"""
    qt_minute_4 """SELECT MINUTE('2023-07-13')"""
    qt_minute_5 """SELECT MINUTE(NULL)"""
    
    // 47. MINUTES_ADD function tests
    qt_minutes_add_1 """SELECT MINUTES_ADD('2020-02-02', 1)"""
    qt_minutes_add_2 """SELECT MINUTES_ADD('2023-07-13 22:28:18', 5)"""
    qt_minutes_add_3 """SELECT MINUTES_ADD('2023-07-13 22:28:18.456789', 10)"""
    qt_minutes_add_4 """SELECT MINUTES_ADD('2023-07-13 22:28:18', -5)"""
    qt_minutes_add_5 """SELECT MINUTES_ADD(NULL, 10), MINUTES_ADD('2023-07-13 22:28:18', NULL)"""
    qt_minutes_add_6 """SELECT MINUTES_ADD('2023-07-13 22:28:18', 2147483648)"""
    qt_minutes_add_7 """SELECT MINUTES_ADD('2023-12-31 23:59:59+08:00', 30)"""
    qt_minutes_add_8 """SELECT MINUTES_ADD('2023-01-01 00:00:00Z', 60)"""
    qt_minutes_add_9 """SELECT MINUTES_ADD('2024-02-29 12:34:56.123-05:00', 45)"""
    qt_minutes_add_10 """SELECT MINUTES_ADD('0001-01-01 00:00:00+00:00', 1)"""
    qt_minutes_add_11 """SELECT MINUTES_ADD('2023-06-15 15:45:30.555+09:00', -30)"""
    qt_minutes_add_12 """SELECT MINUTES_ADD('9999-12-31 23:59:59-12:00', 15)"""
    qt_minutes_add_13 """SELECT MINUTES_ADD('2023-03-15 10:20:30+05:30', -60)"""

    // 48. MINUTES_DIFF function tests
    qt_minutes_diff_1 """SELECT MINUTES_DIFF('2020-12-25 22:00:00', '2020-12-25 21:00:00')"""
    qt_minutes_diff_2 """SELECT MINUTES_DIFF('2020-12-25 21:00:00.999', '2020-12-25 22:00:00.923')"""
    qt_minutes_diff_3 """SELECT MINUTES_DIFF('2023-07-13 21:50:00', '2023-07-13 22:00:00')"""
    qt_minutes_diff_4 """SELECT MINUTES_DIFF('2023-07-14', '2023-07-13')"""
    qt_minutes_diff_5 """SELECT MINUTES_DIFF('2023-07-13 22:30:59', '2023-07-13 22:31:01')"""
    qt_minutes_diff_6 """SELECT MINUTES_DIFF(NULL, '2023-07-13 22:00:00'), MINUTES_DIFF('2023-07-13 22:00:00', NULL)"""

    // 49. MINUTES_SUB function tests
    qt_minutes_sub_1 """SELECT MINUTES_SUB('2020-02-02 02:02:02', 1)"""
    qt_minutes_sub_2 """SELECT MINUTES_SUB('2023-07-13 22:38:18.456789', 10)"""
    qt_minutes_sub_3 """SELECT MINUTES_SUB('2023-07-13 22:23:18', -5)"""
    qt_minutes_sub_4 """SELECT MINUTES_SUB('2023-07-13', 30)"""
    qt_minutes_sub_5 """SELECT MINUTES_SUB(NULL, 10), MINUTES_SUB('2023-07-13 22:28:18', NULL)"""
    qt_minutes_sub_6 """SELECT MINUTES_SUB('2023-12-31 23:59:59+08:00', 30)"""
    qt_minutes_sub_7 """SELECT MINUTES_SUB('2023-01-01 00:00:00Z', 60)"""
    qt_minutes_sub_8 """SELECT MINUTES_SUB('2024-02-29 12:34:56.123-05:00', 45)"""
    qt_minutes_sub_9 """SELECT MINUTES_SUB('0001-01-01 00:00:00+00:00', 1)"""
    qt_minutes_sub_10 """SELECT MINUTES_SUB('2023-06-15 15:45:30.555+09:00', -30)"""
    qt_minutes_sub_11 """SELECT MINUTES_SUB('9999-12-31 23:59:59-12:00', 15)"""
    qt_minutes_sub_12 """SELECT MINUTES_SUB('2023-03-15 10:20:30+05:30', -60)"""
    
    // 50. MONTH_CEIL function tests
    qt_month_ceil_1 """SELECT MONTH_CEIL('2023-07-13 22:28:18')"""
    qt_month_ceil_2 """SELECT MONTH_CEIL('2023-07-13 22:28:18', 5)"""
    qt_month_ceil_3 """SELECT MONTH_CEIL('2023-12-01 00:00:00', 5)"""
    qt_month_ceil_4 """SELECT MONTH_CEIL('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00')"""
    qt_month_ceil_5 """SELECT MONTH_CEIL('2023-07-13 22:28:18.456789', 5)"""
    qt_month_ceil_6 """SELECT MONTH_CEIL('2023-07-13', 3)"""
    qt_month_ceil_7 """SELECT MONTH_CEIL(NULL, 5), MONTH_CEIL('2023-07-13 22:28:18', NULL)"""
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 22:28:18+05:00')", '+12:45')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-12-31 20:00:00+00:00', '2023-01-01 00:00:00+08:00')", '-11:00')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 22:28:18+05:00', 3, '0001-01-01 00:00:00')", '+05:45')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('9999-06-15 10:30:45+05:00', 3, '9999-01-01 00:00:00')", '-03:30')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-01-31 23:59:59.999+08:00', 3)", '+01:30')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-11-30 23:59:59.999+08:00', 3, '2023-01-01 00:00:00')", '-09:00')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2024-02-29 23:59:59.999+08:00', 3)", '+10:15')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-12-31 23:59:59.999+00:00', 3)", '-06:30')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('0001-12-31 23:59:59.999+08:00', 3)", '+08:30')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-01-01 00:00:00.000001+08:00', 3)", '-01:15')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-06-30 23:59:59.999999+08:00', 3)", '+07:00')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 22:28:18', '2023-01-01 00:00:00+08:00')", '-12:00')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 22:28:18+05:00', 5)", '+14:00')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 22:28:18+05:00', 5, '2023-01-01')", '-07:00')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00+08:00')", '+03:45')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('0001-01-15 12:00:00+08:00')", '-02:45')
    validateTimestamptzCeilFloor("SELECT MONTH_CEIL('2023-07-13 23:59:59+00:00', 5, '2023-01-01 00:00:00+08:00')", '+09:30')
    // 51. MONTH_FLOOR function tests
    qt_month_floor_1 """SELECT MONTH_FLOOR('2023-07-13 22:28:18')"""
    qt_month_floor_2 """SELECT MONTH_FLOOR('2023-07-13 22:28:18', 5)"""
    qt_month_floor_3 """SELECT MONTH_FLOOR('2023-06-01 00:00:00', 5)"""
    qt_month_floor_4 """SELECT MONTH_FLOOR('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00')"""
    qt_month_floor_5 """SELECT MONTH_FLOOR('2023-07-13 22:28:18.456789', 5)"""
    qt_month_floor_6 """SELECT MONTH_FLOOR('2023-07-13', 3)"""
    qt_month_floor_7 """SELECT MONTH_FLOOR(NULL, 5), MONTH_FLOOR('2023-07-13 22:28:18', NULL)"""
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 22:28:18+05:00')", '+11:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-12-31 20:00:00+00:00', '2023-01-01 00:00:00+08:00')", '-10:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 22:28:18+05:00', 3, '0001-01-01 00:00:00')", '+04:30')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('9999-06-15 10:30:45+05:00', 3, '9999-01-01 00:00:00')", '-09:30')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-01-31 23:59:59.999+08:00', 3)", '+12:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-11-30 23:59:59.999+08:00', 3, '2023-01-01 00:00:00')", '-08:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2024-02-29 23:59:59.999+08:00', 3)", '+05:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-12-31 23:59:59.999+00:00', 3)", '-04:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('0001-12-31 23:59:59.999+08:00', 3)", '+09:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('9999-12-31 12:00:00.123+08:00', 2)", '-06:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-01-01 00:00:00.000001+08:00', 3)", '+02:00')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-06-30 23:59:59.999999+08:00', 3)", '-11:30')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 22:28:18', '2023-01-01 00:00:00+08:00')", '+13:45')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 22:28:18+05:00', 5)", '-03:15')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 22:28:18+05:00', 5, '2023-01-01')", '+06:15')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00+08:00')", '-01:45')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('0001-01-15 12:00:00+08:00')", '+08:15')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('9999-12-15 12:00:00+08:00')", '-00:45')
    validateTimestamptzCeilFloor("SELECT MONTH_FLOOR('2023-07-13 23:59:59+00:00', 5, '2023-01-01 00:00:00+08:00')", '+11:45')
    // 52. MONTH function tests
    qt_month_1 """SELECT MONTH('1987-01-01')"""
    qt_month_2 """SELECT MONTH('2023-07-13 22:28:18')"""
    qt_month_3 """SELECT MONTH('2023-12-05 10:15:30.456789')"""
    qt_month_4 """SELECT MONTH(NULL)"""
    
    // 53. MONTHNAME function tests
    qt_monthname_1 """SELECT MONTHNAME('2008-02-03')"""
    qt_monthname_2 """SELECT MONTHNAME('2023-07-13 22:28:18')"""
    qt_monthname_3 """SELECT MONTHNAME(NULL)"""
    
    sql """SET lc_time_names='zh_CN'"""
    qt_monthname_zh_cn """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='zh_TW'"""
    qt_monthname_zh_tw """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='zh_HK'"""
    qt_monthname_zh_hk """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='FR_fr'"""
    qt_monthname_fr_fr """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='fr_BE'"""
    qt_monthname_fr_be """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='fr_CA'"""
    qt_monthname_fr_ca """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='fr_CH'"""
    qt_monthname_fr_ch """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='de_DE'"""
    qt_monthname_de_de """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='de_AT'"""
    qt_monthname_de_at """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='DE_ch'"""
    qt_monthname_de_ch """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='JA_jp'"""
    qt_monthname_ja_jp """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='Ko_KR'"""
    qt_monthname_ko_kr """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='es_ES'"""
    qt_monthname_es_es """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='es_MX'"""
    qt_monthname_es_mx """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='Es_Ar'"""
    qt_monthname_es_ar """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='es_CO'"""
    qt_monthname_es_co """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='IT_it'"""
    qt_monthname_it_it """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='it_CH'"""
    qt_monthname_it_ch """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ru_RU'"""
    qt_monthname_ru_ru """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='RU_ua'"""
    qt_monthname_ru_ua """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ar_SA'"""
    qt_monthname_ar_sa """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ar_AE'"""
    qt_monthname_ar_ae """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='AR_eg'"""
    qt_monthname_ar_eg """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ar_JO'"""
    qt_monthname_ar_jo """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='en_US'"""
    qt_monthname_en_us """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='en_GB'"""
    qt_monthname_en_gb """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='En_Au'"""
    qt_monthname_en_au """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='en_CA'"""
    qt_monthname_en_ca """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='pt_BR'"""
    qt_monthname_pt_br """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='pt_PT'"""
    qt_monthname_pt_pt """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='nl_NL'"""
    qt_monthname_nl_nl """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='NL_be'"""
    qt_monthname_nl_be """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='sv_SE'"""
    qt_monthname_sv_se """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='sv_FI'"""
    qt_monthname_sv_fi """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='no_NO'"""
    qt_monthname_no_no """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='nb_NO'"""
    qt_monthname_nb_no """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='da_DK'"""
    qt_monthname_da_dk """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='fi_FI'"""
    qt_monthname_fi_fi """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='pl_PL'"""
    qt_monthname_pl_pl """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='cs_CZ'"""
    qt_monthname_cs_cz """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='sk_SK'"""
    qt_monthname_sk_sk """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='hu_HU'"""
    qt_monthname_hu_hu """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ro_RO'"""
    qt_monthname_ro_ro """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='bg_BG'"""
    qt_monthname_bg_bg """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='hr_HR'"""
    qt_monthname_hr_hr """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='sl_SI'"""
    qt_monthname_sl_si """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='sr_RS'"""
    qt_monthname_sr_rs """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='mk_MK'"""
    qt_monthname_mk_mk """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='sq_AL'"""
    qt_monthname_sq_al """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='th_TH'"""
    qt_monthname_th_th """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='vi_VN'"""
    qt_monthname_vi_vn """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='tr_TR'"""
    qt_monthname_tr_tr """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='el_GR'"""
    qt_monthname_el_gr """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='he_IL'"""
    qt_monthname_he_il """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='hi_IN'"""
    qt_monthname_hi_in """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='gu_IN'"""
    qt_monthname_gu_in """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ta_IN'"""
    qt_monthname_ta_in """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='te_IN'"""
    qt_monthname_te_in """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='id_ID'"""
    qt_monthname_id_id """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ms_MY'"""
    qt_monthname_ms_my """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='lt_LT'"""
    qt_monthname_lt_lt """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='lv_LV'"""
    qt_monthname_lv_lv """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='et_EE'"""
    qt_monthname_et_ee """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='is_IS'"""
    qt_monthname_is_is """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='fo_FO'"""
    qt_monthname_fo_fo """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='be_BY'"""
    qt_monthname_be_by """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='uk_UA'"""
    qt_monthname_uk_ua """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='mn_MN'"""
    qt_monthname_mn_mn """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ur_PK'"""
    qt_monthname_ur_pk """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ca_ES'"""
    qt_monthname_ca_es """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='eu_ES'"""
    qt_monthname_eu_es """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='gl_ES'"""
    qt_monthname_gl_es """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='rm_CH'"""
    qt_monthname_rm_ch """SELECT MONTHNAME('2023-07-13')"""
    
    sql """SET lc_time_names='ar_AE'"""
    testFoldConst("SELECT MONTHNAME('2023-01-15');")
    
    sql """SET lc_time_names='zh_CN'"""
    testFoldConst("SELECT MONTHNAME('2023-02-20');")
    
    sql """SET lc_time_names='ja_JP'"""
    testFoldConst("SELECT MONTHNAME('2023-03-25');")
    
    sql """SET lc_time_names='ko_KR'"""
    testFoldConst("SELECT MONTHNAME('2023-04-10');")
    
    sql """SET lc_time_names='ru_RU'"""
    testFoldConst("SELECT MONTHNAME('2023-05-18');")
    
    sql """SET lc_time_names='de_DE'"""
    testFoldConst("SELECT MONTHNAME('2023-06-22');")
    
    sql """SET lc_time_names='fr_FR'"""
    testFoldConst("SELECT MONTHNAME('2023-07-13');")
    
    sql """SET lc_time_names='es_ES'"""
    testFoldConst("SELECT MONTHNAME('2023-08-05');")
    
    sql """SET lc_time_names='pt_BR'"""
    testFoldConst("SELECT MONTHNAME('2023-09-30');")
    
    sql """SET lc_time_names='it_IT'"""
    testFoldConst("SELECT MONTHNAME('2023-10-12');")
    
    sql """SET lc_time_names='nl_NL'"""
    testFoldConst("SELECT MONTHNAME('2023-11-28');")
    
    sql """SET lc_time_names='sv_SE'"""
    testFoldConst("SELECT MONTHNAME('2023-12-01');")
    
    sql """SET lc_time_names='pl_PL'"""
    testFoldConst("SELECT MONTHNAME('2024-01-31');")
    
    sql """SET lc_time_names='cs_CZ'"""
    testFoldConst("SELECT MONTHNAME('2024-02-29');")
    
    sql """SET lc_time_names='bg_BG'"""
    testFoldConst("SELECT MONTHNAME('2024-03-15');")
    
    sql """SET lc_time_names='uk_UA'"""
    testFoldConst("SELECT MONTHNAME('2024-04-01');")

    sql """SET lc_time_names=default"""
    
    // 54. MONTHS_ADD function tests
    qt_months_add_1 """SELECT MONTHS_ADD('2020-01-31', 1)"""
    qt_months_add_2 """SELECT MONTHS_ADD('2020-01-31 02:02:02', 1)"""
    qt_months_add_3 """SELECT MONTHS_ADD('2020-01-31', -1)"""
    qt_months_add_4 """SELECT MONTHS_ADD('2023-07-13 22:28:18', 5)"""
    qt_months_add_5 """SELECT MONTHS_ADD('2023-07-13 22:28:18.456789', 3)"""
    qt_months_add_6 """SELECT MONTHS_ADD(NULL, 5), MONTHS_ADD('2023-07-13', NULL)"""
    qt_months_add_7 """SELECT MONTHS_ADD('2023-12-31 23:59:59+08:00', 1)"""
    qt_months_add_8 """SELECT MONTHS_ADD('2023-01-15 10:30:45.000000Z', 2)"""
    qt_months_add_9 """SELECT MONTHS_ADD('2024-02-29 15:45:22.999999-05:00', 1)"""
    qt_months_add_10 """SELECT MONTHS_ADD('0001-01-01 00:00:00+00:00', 12)"""
    qt_months_add_11 """SELECT MONTHS_ADD('2023-06-30 23:59:59.555555+09:00', -6)"""
    qt_months_add_12 """SELECT MONTHS_ADD('9999-12-31 12:00:00-12:00', 6)"""
    qt_months_add_13 """SELECT MONTHS_ADD('2023-03-31 10:20:30+05:30', 3)"""
    
    // 55. MONTHS_BETWEEN function tests
    qt_months_between_1 """SELECT MONTHS_BETWEEN('2020-12-26', '2020-10-25')"""
    qt_months_between_2 """SELECT MONTHS_BETWEEN('2020-12-26 15:30:00', '2020-10-25 08:15:00')"""
    qt_months_between_3 """SELECT MONTHS_BETWEEN('2020-10-25', '2020-12-26', false)"""
    qt_months_between_4 """SELECT MONTHS_BETWEEN('2024-02-29', '2024-01-31')"""
    qt_months_between_5 """SELECT MONTHS_BETWEEN('2024-03-15', '2024-01-15')"""
    qt_months_between_6 """SELECT MONTHS_BETWEEN('2024-02-29', '2024-01-30')"""
    qt_months_between_7 """SELECT MONTHS_BETWEEN(NULL, '2024-01-01')"""
    
    // 56. MONTHS_DIFF function tests
    qt_months_diff_1 """SELECT MONTHS_DIFF('2020-03-28', '2020-02-29')"""
    qt_months_diff_2 """SELECT MONTHS_DIFF('2020-03-29', '2020-02-29')"""
    qt_months_diff_3 """SELECT MONTHS_DIFF('2020-03-30', '2020-02-29')"""
    qt_months_diff_4 """SELECT MONTHS_DIFF('2020-02-29', '2020-03-28')"""
    qt_months_diff_5 """SELECT MONTHS_DIFF('2020-02-29', '2020-03-29')"""
    qt_months_diff_6 """SELECT MONTHS_DIFF('2023-07-15', '2023-07-30')"""
    qt_months_diff_7 """SELECT MONTHS_DIFF(NULL, '2023-01-01')"""
    
    // 57. MONTHS_SUB function tests
    qt_months_sub_1 """SELECT MONTHS_SUB('2020-01-31', 1)"""
    qt_months_sub_2 """SELECT MONTHS_SUB('2020-01-31 02:02:02', 1)"""
    qt_months_sub_3 """SELECT MONTHS_SUB('2020-01-31', -1)"""
    qt_months_sub_4 """SELECT MONTHS_SUB('2023-07-13 22:28:18', 5)"""
    qt_months_sub_5 """SELECT MONTHS_SUB('2023-10-13 22:28:18.456789', 3)"""
    qt_months_sub_6 """SELECT MONTHS_SUB(NULL, 5), MONTHS_SUB('2023-07-13', NULL)"""
    qt_months_sub_7 """SELECT MONTHS_SUB('2023-12-31 23:59:59+08:00', 1)"""
    qt_months_sub_8 """SELECT MONTHS_SUB('2023-01-15 10:30:45.000000Z', 2)"""
    qt_months_sub_9 """SELECT MONTHS_SUB('2024-02-29 15:45:22.999999-05:00', 1)"""
    qt_months_sub_10 """SELECT MONTHS_SUB('0001-01-01 00:00:00+00:00', 12)"""
    qt_months_sub_11 """SELECT MONTHS_SUB('2023-06-30 23:59:59.555555+09:00', -6)"""
    qt_months_sub_12 """SELECT MONTHS_SUB('9999-12-31 12:00:00-12:00', 6)"""
    qt_months_sub_13 """SELECT MONTHS_SUB('2023-03-31 10:20:30+05:30', 3)"""
    
    // 58. NEXT_DAY function tests
    qt_next_day_1 """SELECT NEXT_DAY('2020-01-31', 'MONDAY')"""
    qt_next_day_2 """SELECT NEXT_DAY('2020-01-31 02:02:02', 'MON')"""
    qt_next_day_3 """SELECT NEXT_DAY('2023-07-17', 'MON')"""
    qt_next_day_4 """SELECT NEXT_DAY('2023-07-13', 'FR')"""
    qt_next_day_5 """SELECT NEXT_DAY(NULL, 'SUN')"""
    qt_next_day_6 """SELECT NEXT_DAY('9999-12-31 12:00:00', 'SUNDAY')"""
    
    
    // 60. QUARTER function tests
    qt_quarter_1 """SELECT QUARTER('2025-01-16')"""
    qt_quarter_2 """SELECT QUARTER('2025-01-16 01:11:10')"""
    qt_quarter_3 """SELECT QUARTER('2023-05-20')"""
    qt_quarter_4 """SELECT QUARTER('2024-09-30 23:59:59')"""
    qt_quarter_5 """SELECT QUARTER('2022-12-01')"""
    qt_quarter_6 """SELECT QUARTER(NULL)"""
    
    // 61. QUARTERS_ADD function tests
    qt_quarters_add_1 """SELECT QUARTERS_ADD('2020-01-31', 1)"""
    qt_quarters_add_2 """SELECT QUARTERS_ADD('2020-01-31 02:02:02', 1)"""
    qt_quarters_add_3 """SELECT QUARTERS_ADD('2020-04-30', -1)"""
    qt_quarters_add_4 """SELECT QUARTERS_ADD('2023-07-13 22:28:18', 2)"""
    qt_quarters_add_5 """SELECT QUARTERS_ADD('2023-07-13 22:28:18.456789', 1)"""
    qt_quarters_add_6 """SELECT QUARTERS_ADD('2023-10-01', 2)"""
    qt_quarters_add_7 """SELECT QUARTERS_ADD(NULL, 1), QUARTERS_ADD('2023-07-13', NULL)"""
    qt_quarters_add_8 """SELECT QUARTERS_ADD('2023-12-31 23:59:59+08:00', 1)"""
    qt_quarters_add_9 """SELECT QUARTERS_ADD('2023-01-01 00:00:00Z', 2)"""
    qt_quarters_add_10 """SELECT QUARTERS_ADD('2024-02-29 12:30:45.123-05:00', 1)"""
    qt_quarters_add_11 """SELECT QUARTERS_ADD('0001-01-01 00:00:00+00:00', 4)"""
    qt_quarters_add_12 """SELECT QUARTERS_ADD('2023-06-15 15:45:30.555+09:00', -2)"""
    qt_quarters_add_13 """SELECT QUARTERS_ADD('9999-12-31 12:00:00-12:00', 1)"""
    
    // 62. QUARTERS_SUB function tests
    qt_quarters_sub_1 """SELECT QUARTERS_SUB('2020-01-31', 1)"""
    qt_quarters_sub_2 """SELECT QUARTERS_SUB('2020-01-31 02:02:02', 1)"""
    qt_quarters_sub_3 """SELECT QUARTERS_SUB('2019-10-31', -1)"""
    qt_quarters_sub_4 """SELECT QUARTERS_SUB('2023-07-13 22:28:18', 2)"""
    qt_quarters_sub_5 """SELECT QUARTERS_SUB('2023-10-13 22:28:18.456789', 1)"""
    qt_quarters_sub_6 """SELECT QUARTERS_SUB('2024-04-01', 2)"""
    qt_quarters_sub_7 """SELECT QUARTERS_SUB(NULL, 1), QUARTERS_SUB('2023-07-13', NULL)"""
    qt_quarters_sub_8 """SELECT QUARTERS_SUB('2023-12-31 23:59:59+08:00', 1)"""
    qt_quarters_sub_9 """SELECT QUARTERS_SUB('2023-01-01 00:00:00Z', 2)"""
    qt_quarters_sub_10 """SELECT QUARTERS_SUB('2024-02-29 12:30:45.123-05:00', 1)"""
    qt_quarters_sub_11 """SELECT QUARTERS_SUB('0001-01-01 00:00:00+00:00', 4)"""
    qt_quarters_sub_12 """SELECT QUARTERS_SUB('2023-06-15 15:45:30.555+09:00', -2)"""
    qt_quarters_sub_13 """SELECT QUARTERS_SUB('9999-12-31 12:00:00-12:00', 1)"""

    // Group 5: Second and time manipulation functions (序号 63-77)
    
    // 63. SECOND_CEIL function tests
    qt_second_ceil_1 """SELECT SECOND_CEIL('2025-01-23 12:34:56')"""
    qt_second_ceil_2 """SELECT SECOND_CEIL('2025-01-23 12:34:56', 5)"""
    qt_second_ceil_3 """SELECT SECOND_CEIL('2025-01-23 12:34:56', 10, '2025-01-23 12:00:00')"""
    qt_second_ceil_4 """SELECT SECOND_CEIL('2025-01-23 12:34:56.789', 5)"""
    qt_second_ceil_5 """SELECT SECOND_CEIL('2025-01-23', 30)"""
    qt_second_ceil_6 """SELECT SECOND_CEIL(NULL, 5), SECOND_CEIL('2025-01-23 12:34:56', NULL)"""
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-07-13 22:28:18.456789+05:00', 5)", '+10:30')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-07-13 22:28:18.456789+05:00', 10, '2023-07-13 22:28:00')", '-09:00')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-07-13 22:28:18.654321', 10, '2023-07-13 22:28:00+08:00')", '+07:45')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('0001-01-01 00:00:00.000001+08:00', 5)", '-05:00')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2025-12-31 23:59:59.999+00:00', 20, '2025-12-31 23:59:00+08:00')", '+03:30')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-06-30 23:59:59.500+00:00', 30)", '-02:00')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-07-13 22:28:18+05:00', 30, '0001-01-01 00:00:00')", '+00:15')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('9999-12-31 10:30:45+05:00', 30, '9999-12-31 10:30:00')", '-11:45')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-01-01 23:59:59.999+08:00', 30)", '+12:30')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-06-30 23:59:59.999+08:00', 30, '2023-06-30 23:59:00')", '-08:15')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-12-31 23:59:59.999+00:00', 30)", '+05:30')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('0001-01-01 23:59:59.999+08:00', 20)", '-04:45')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-07-13 12:00:00.000001+08:00', 30)", '+09:45')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('2023-07-13 23:59:59.999999+08:00', 30)", '-06:15')
    validateTimestamptzCeilFloor("SELECT SECOND_CEIL('9999-12-31 20:55:59+05:00');", '+01:00')

    // 64. SECOND_FLOOR function tests
    qt_second_floor_1 """SELECT SECOND_FLOOR('2025-01-23 12:34:56')"""
    qt_second_floor_2 """SELECT SECOND_FLOOR('2025-01-23 12:34:56', 5)"""
    qt_second_floor_3 """SELECT SECOND_FLOOR('2025-01-23 12:34:56', 10, '2025-01-23 12:00:00')"""
    qt_second_floor_4 """SELECT SECOND_FLOOR('2025-01-23 12:34:56.789', 5)"""
    qt_second_floor_5 """SELECT SECOND_FLOOR('2025-01-23', 30)"""
    qt_second_floor_6 """SELECT SECOND_FLOOR(NULL, 5), SECOND_FLOOR('2025-01-23 12:34:56', NULL)"""
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-07-13 22:28:18.456789+05:00', 5)", '+11:15')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-07-13 22:28:18.456789+05:00', 10, '2023-07-13 22:28:00')", '-10:45')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-07-13 22:28:18.654321', 10, '2023-07-13 22:28:00+08:00')", '+04:15')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('0001-01-01 00:00:00.000001+08:00', 5)", '-03:45')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('9999-12-31 23:59:59.999999-02:00', 5)", '+08:00')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2025-12-31 23:59:59.999+00:00', 20, '2025-12-31 23:59:00+08:00')", '-07:15')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-06-30 23:59:59.500+00:00', 30)", '+13:30')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-07-13 22:28:18+05:00', 30, '0001-01-01 00:00:00')", '-00:15')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('9999-12-31 10:30:45+05:00', 30, '9999-12-31 10:30:00')", '+06:45')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-01-01 23:59:59.999+08:00', 30)", '-05:15')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-06-30 23:59:59.999+08:00', 30, '2023-06-30 23:59:00')", '+02:45')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-12-31 23:59:59.999+00:00', 30)", '-01:30')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('0001-01-01 23:59:59.999+08:00', 20)", '+09:00')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('9999-12-31 23:59:59.123+08:00', 20)", '-08:30')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-07-13 12:00:00.000001+08:00', 30)", '+05:15')
    validateTimestamptzCeilFloor("SELECT SECOND_FLOOR('2023-07-13 23:59:59.999999+08:00', 30)", '-06:45')

    // 65. SECOND function tests
    qt_second_1 """SELECT SECOND('2018-12-31 23:59:59')"""
    qt_second_2 """SELECT SECOND(cast('15:42:33' as time))"""
    qt_second_3 """SELECT SECOND('2023-07-13')"""
    qt_second_4 """SELECT SECOND('2023-07-13 10:30:25.123456')"""
    qt_second_5 """SELECT SECOND('2024-01-01 00:00:00')"""
    qt_second_6 """SELECT SECOND(NULL)"""
    
    // 66. SECONDS_ADD function tests
    qt_seconds_add_1 """SELECT SECONDS_ADD('2025-01-23 12:34:56', 30)"""
    qt_seconds_add_2 """SELECT SECONDS_ADD('2025-01-23 12:34:56', -30)"""
    qt_seconds_add_3 """SELECT SECONDS_ADD('2023-07-13 23:59:50', 15)"""
    qt_seconds_add_4 """SELECT SECONDS_ADD('2023-01-01', 3600)"""
    qt_seconds_add_5 """SELECT SECONDS_ADD('2023-07-13 10:30:25.123456', 2)"""
    qt_seconds_add_6 """SELECT SECONDS_ADD(NULL, 30), SECONDS_ADD('2025-01-23 12:34:56', NULL)"""
    qt_seconds_add_7 """SELECT SECONDS_ADD('2023-12-31 23:59:59+08:00', 30)"""
    qt_seconds_add_8 """SELECT SECONDS_ADD('2023-01-01 00:00:00Z', 3600)"""
    qt_seconds_add_9 """SELECT SECONDS_ADD('2024-02-29 12:34:56.123-05:00', 15)"""
    qt_seconds_add_10 """SELECT SECONDS_ADD('0001-01-01 00:00:00+00:00', 60)"""
    qt_seconds_add_11 """SELECT SECONDS_ADD('2023-06-15 15:45:30.555+09:00', -30)"""
    qt_seconds_add_12 """SELECT SECONDS_ADD('9999-12-31 23:59:59-12:00', 1)"""
    qt_seconds_add_13 """SELECT SECONDS_ADD('2023-03-15 10:20:30+05:30', -60)"""

    // 67. SECONDS_DIFF function tests
    qt_seconds_diff_1 """SELECT SECONDS_DIFF('2025-01-23 12:35:56', '2025-01-23 12:34:56')"""
    qt_seconds_diff_2 """SELECT SECONDS_DIFF('2023-01-01 00:00:00', '2023-01-01 00:01:00')"""
    qt_seconds_diff_3 """SELECT SECONDS_DIFF('2023-01-02', '2023-01-01')"""
    qt_seconds_diff_4 """SELECT SECONDS_DIFF('2023-07-13 12:00:00.123', '2023-07-13 11:59:59')"""
    qt_seconds_diff_5 """SELECT SECONDS_DIFF(NULL, '2023-07-13 10:30:25'), SECONDS_DIFF('2023-07-13 10:30:25', NULL)"""

    // 68. SECONDS_SUB function tests
    qt_seconds_sub_1 """SELECT SECONDS_SUB('2025-01-23 12:34:56', 30)"""
    qt_seconds_sub_2 """SELECT SECONDS_SUB('2025-01-23 12:34:56', -30)"""
    qt_seconds_sub_3 """SELECT SECONDS_SUB('2023-07-14 00:00:10', 15)"""
    qt_seconds_sub_4 """SELECT SECONDS_SUB('2023-01-01', 3600)"""
    qt_seconds_sub_5 """SELECT SECONDS_SUB('2023-07-13 10:30:25.123456', 2)"""
    qt_seconds_sub_6 """SELECT SECONDS_SUB(NULL, 30), SECONDS_SUB('2025-01-23 12:34:56', NULL)"""
    qt_seconds_sub_7 """SELECT SECONDS_SUB('2023-12-31 23:59:59+08:00', 30)"""
    qt_seconds_sub_8 """SELECT SECONDS_SUB('2023-01-01 00:00:00Z', 3600)"""
    qt_seconds_sub_9 """SELECT SECONDS_SUB('2024-02-29 12:34:56.123-05:00', 15)"""
    qt_seconds_sub_10 """SELECT SECONDS_SUB('0001-01-01 00:00:00+00:00', 60)"""
    qt_seconds_sub_11 """SELECT SECONDS_SUB('2023-06-15 15:45:30.555+09:00', -30)"""
    qt_seconds_sub_12 """SELECT SECONDS_SUB('9999-12-31 23:59:59-12:00', 1)"""
    qt_seconds_sub_13 """SELECT SECONDS_SUB('2023-03-15 10:20:30+05:30', -60)"""
    
    // 69. SECOND_TIMESTAMP function tests
    qt_second_timestamp_1 """SELECT SECOND_TIMESTAMP('1970-01-01 00:00:00 UTC')"""
    qt_second_timestamp_2 """SELECT SECOND_TIMESTAMP('2025-01-23 12:34:56')"""
    qt_second_timestamp_3 """SELECT SECOND_TIMESTAMP('2023-01-01')"""
    qt_second_timestamp_4 """SELECT SECOND_TIMESTAMP('1964-10-31 23:59:59')"""
    qt_second_timestamp_5 """SELECT SECOND_TIMESTAMP('2023-07-13 22:28:18.456789')"""
    qt_second_timestamp_6 """SELECT SECOND_TIMESTAMP(NULL)"""
    
    // 70. SEC_TO_TIME function tests
    qt_sec_to_time_1 """SELECT SEC_TO_TIME(59738)"""
    qt_sec_to_time_2 """SELECT SEC_TO_TIME(90061)"""
    qt_sec_to_time_3 """SELECT SEC_TO_TIME(-3600)"""
    qt_sec_to_time_4 """SELECT SEC_TO_TIME(0)"""
    qt_sec_to_time_5 """SELECT SEC_TO_TIME(3661.9)"""
    qt_sec_to_time_6 """SELECT SEC_TO_TIME(NULL)"""
    
    // 71. STR_TO_DATE function tests
    qt_str_to_date_1 """SELECT STR_TO_DATE('2025-01-23 12:34:56', '%Y-%m-%d %H:%i:%s')"""
    qt_str_to_date_2 """SELECT STR_TO_DATE('2025-01-23 12:34:56', 'yyyy-MM-dd HH:mm:ss')"""
    qt_str_to_date_3 """SELECT STR_TO_DATE('20230713', 'yyyyMMdd')"""
    qt_str_to_date_4 """SELECT STR_TO_DATE('15:30:45', '%H:%i:%s')"""
    qt_str_to_date_5 """SELECT STR_TO_DATE('200442 Monday', '%X%V %W')"""
    qt_str_to_date_6 """SELECT STR_TO_DATE('Oct 5 2023 3:45:00 PM', '%b %d %Y %h:%i:%s %p')"""
    qt_str_to_date_7 """SELECT STR_TO_DATE('2023/01/01', '%Y-%m-%d')"""
    qt_str_to_date_8 """SELECT STR_TO_DATE('2023-01-01 10:00:00 (GMT)', '%Y-%m-%d %H:%i:%s')"""
    qt_str_to_date_9 """SELECT STR_TO_DATE('2023-07-13 12:34:56.789', '%Y-%m-%d %H:%i:%s.%f')"""
    qt_str_to_date_10 """SELECT STR_TO_DATE(NULL, '%Y-%m-%d'), STR_TO_DATE('2023-01-01', NULL)"""
    qt_str_to_date_11 """SELECT STR_TO_DATE('2023-01-01', '')"""
    
    // 72. TIMEDIFF function tests
    qt_timediff_1 """SELECT TIMEDIFF('2024-07-20 16:59:30', '2024-07-11 16:35:21')"""
    qt_timediff_2 """SELECT TIMEDIFF('2023-10-05 15:45:00', '2023-10-05')"""
    qt_timediff_3 """SELECT TIMEDIFF('2023-01-01 09:00:00', '2023-01-01 10:30:00')"""
    qt_timediff_4 """SELECT TIMEDIFF('2023-12-31 23:59:59', '2023-12-31 23:59:50')"""
    qt_timediff_5 """SELECT TIMEDIFF('2024-01-01 00:00:01', '2023-12-31 23:59:59')"""
    qt_timediff_6 """SELECT TIMEDIFF('2023-07-13 12:34:56.789', '2023-07-13 12:34:50.123')"""
    qt_timediff_7 """SELECT TIMEDIFF(NULL, '2023-01-01 00:00:00'), TIMEDIFF('2023-01-01 00:00:00', NULL)"""
    
    // 73. TIME function tests
    qt_time_1 """SELECT TIME('2025-1-1 12:12:12')"""
    
    // 74. TIMESTAMPADD function tests
    qt_timestampadd_1 """SELECT TIMESTAMPADD(MINUTE, 1, '2019-01-02')"""
    qt_timestampadd_2 """SELECT TIMESTAMPADD(WEEK, 1, '2019-01-02')"""
    qt_timestampadd_3 """SELECT TIMESTAMPADD(HOUR, -3, '2023-07-13 10:30:00')"""
    qt_timestampadd_4 """SELECT TIMESTAMPADD(MONTH, 1, '2023-01-31')"""
    qt_timestampadd_5 """SELECT TIMESTAMPADD(YEAR, 1, '2023-12-31 23:59:59')"""
    qt_timestampadd_6 """SELECT TIMESTAMPADD(YEAR,NULL, '2023-12-31 23:59:59')"""
    
    // 75. TIMESTAMPDIFF function tests
    qt_timestampdiff_1 """SELECT TIMESTAMPDIFF(MONTH, '2003-02-01', '2003-05-01')"""
    qt_timestampdiff_2 """SELECT TIMESTAMPDIFF(YEAR, '2002-05-01', '2001-01-01')"""
    qt_timestampdiff_3 """SELECT TIMESTAMPDIFF(MINUTE, '2003-02-01', '2003-05-01 12:05:55')"""
    qt_timestampdiff_4 """SELECT TIMESTAMPDIFF(DAY, '2023-12-31 23:59:50', '2024-01-01 00:00:05')"""
    qt_timestampdiff_5 """SELECT TIMESTAMPDIFF(MONTH, '2023-01-31', '2023-02-28')"""
    qt_timestampdiff_6 """SELECT TIMESTAMPDIFF(MONTH, '2023-01-31', '2023-02-27')"""
    qt_timestampdiff_7 """SELECT TIMESTAMPDIFF(DAY, NULL, '2023-01-01'), TIMESTAMPDIFF(DAY, '2023-01-01', NULL)"""
    qt_timestampdiff_8 """SELECT TIMESTAMPDIFF(WEEK, '2023-01-01', '2023-01-15')"""
    
    // 76. TIMESTAMP function tests
    qt_timestamp_1 """SELECT TIMESTAMP('2019-01-01 12:00:00')"""
    qt_timestamp_2 """SELECT TIMESTAMP('2019-01-01')"""
    qt_timestamp_3 """SELECT TIMESTAMP('2019-01-41 12:00:00')"""
    qt_timestamp_4 """SELECT TIMESTAMP(NULL)"""
    
    // 77. TIME_TO_SEC function tests
    qt_time_to_sec_1 """SELECT TIME_TO_SEC('16:32:18')"""
    qt_time_to_sec_2 """SELECT TIME_TO_SEC('2025-01-01 16:32:18')"""
    qt_time_to_sec_3 """SELECT TIME_TO_SEC('-02:30:00')"""
    qt_time_to_sec_4 """SELECT TIME_TO_SEC('-16:32:18.99')"""
    qt_time_to_sec_5 """SELECT TIME_TO_SEC('10:15:30.123456')"""
    qt_time_to_sec_6 """SELECT TIME_TO_SEC('12:60:00')"""
    qt_time_to_sec_7 """SELECT TIME_TO_SEC('839:00:00')"""
    qt_time_to_sec_8 """SELECT TIME_TO_SEC(NULL)"""

    // Group 6: TO_DATE, TO_DAYS, TO_ISO8601, TO_MONDAY, UNIX_TIMESTAMP, UTC_TIMESTAMP, WEEK_CEIL, WEEKDAY, WEEK_FLOOR, WEEK, WEEKOFYEAR, WEEKS_ADD, WEEKS_DIFF, WEEKS_SUB (序号78-91)
    
    // 78. TO_DATE function tests
    qt_to_date_1 """select to_date("2020-02-02 00:00:00")"""
    qt_to_date_2 """select to_date("2020-02-02")"""
    qt_to_date_3 """SELECT TO_DATE('2023-02-30 23:23:56') AS result"""
    qt_to_date_4 """SELECT TO_DATE(NULL) AS result"""

    // 79. TO_DAYS function tests
    qt_to_days_1 """select to_days('2007-10-07')"""
    qt_to_days_2 """select to_days('2007-10-07 10:03:09')"""
    qt_to_days_3 """select to_days('0000-01-01')"""
    qt_to_days_4 """select to_days('0000-02-28')"""
    qt_to_days_5 """select to_days('0000-02-29')"""
    qt_to_days_6 """select to_days('0000-03-01')"""


    // 80. TO_ISO8601 function tests
    qt_to_iso8601_1 """SELECT TO_ISO8601(CAST('2023-10-05' AS DATE)) AS date_result"""
    qt_to_iso8601_2 """SELECT TO_ISO8601(CAST('2020-01-01 12:30:45' AS DATETIME)) AS datetime_result"""
    qt_to_iso8601_3 """SELECT TO_ISO8601(CAST('2020-01-01 12:30:45.956' AS DATETIME)) AS datetime_result"""
    qt_to_iso8601_4 """SELECT TO_ISO8601('2023-02-30') AS invalid_date"""
    qt_to_iso8601_5 """SELECT TO_ISO8601(NULL) AS null_input"""
    qt_to_iso8601_6 """SELECT TO_ISO8601('2025-10-10 11:22:33+03:00');"""
    qt_to_iso8601_7 """SELECT TO_ISO8601('2025-10-10 11:22:33-05:00');"""
    qt_to_iso8601_8 """SELECT TO_ISO8601('2025-10-10 11:22:33.123Z');"""
    qt_to_iso8601_9 """SELECT TO_ISO8601('2025-10-10 11:22:33.1234567Z');"""
    qt_to_iso8601_10 """SELECT TO_ISO8601('0000-01-01 00:00:00Z');"""

    // 81. TO_MONDAY function tests
    qt_to_monday_1 """SELECT TO_MONDAY('2022-09-10') AS result"""
    qt_to_monday_2 """SELECT TO_MONDAY('1022-09-10') AS result"""
    qt_to_monday_3 """SELECT TO_MONDAY('2023-10-09') AS result"""
    qt_to_monday_4 """SELECT TO_MONDAY('1970-01-02'),TO_MONDAY('1970-01-01'),TO_MONDAY('1970-01-03'),TO_MONDAY('1970-01-04')"""
    qt_to_monday_5 """SELECT TO_MONDAY(NULL) AS result"""

    // 82. UNIX_TIMESTAMP function tests
    qt_unix_timestamp_1 """select unix_timestamp('1970-01-01 +08:00')"""
    qt_unix_timestamp_3 """select unix_timestamp('2007-11-30 10:30:19')"""
    qt_unix_timestamp_4 """select unix_timestamp('2007-11-30 10:30:19 +09:00')"""
    qt_unix_timestamp_5 """select unix_timestamp('2007-11-30 10:30-19', '%Y-%m-%d %H:%i-%s')"""
    qt_unix_timestamp_6 """select unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d')"""
    qt_unix_timestamp_7 """select unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')"""
    qt_unix_timestamp_8 """SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.123')"""
    qt_unix_timestamp_9 """select unix_timestamp('1007-11-30 10:30:19')"""
    qt_unix_timestamp_10 """select unix_timestamp(NULL)"""
    qt_unix_timestamp_11 """select unix_timestamp('2038-01-19 11:14:08',null)"""



    // 84. WEEK_CEIL function tests
    qt_week_ceil_1 """SELECT WEEK_CEIL(cast('2023-07-13 22:28:18' as datetime)) AS result"""
    qt_week_ceil_2 """SELECT WEEK_CEIL('2023-07-13 22:28:18', 2) AS result"""
    qt_week_ceil_3 """SELECT WEEK_CEIL('2023-07-24 22:28:18', 2) AS result"""
    qt_week_ceil_4 """SELECT WEEK_CEIL(cast('2023-07-13' as date))"""
    qt_week_ceil_5 """SELECT WEEK_CEIL('2023-07-13', 1, '2023-07-03') AS result"""
    qt_week_ceil_6 """SELECT WEEK_CEIL('2023-07-10', 1, '2023-07-10 12:00:00') AS result"""
    qt_week_ceil_7 """SELECT WEEK_CEIL(NULL, 1) AS result"""
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00')", '+11:30')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-31 20:00:00+00:00', '2023-07-03 00:00:00+08:00')", '-10:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00', 2, '0001-01-01 00:00:00')", '+04:45')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('9999-06-15 10:30:45+05:00', 2, '9999-06-07 00:00:00')", '-03:30')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-01-07 23:59:59.999+08:00', 2)", '+09:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-06-30 23:59:59.999+08:00', 2, '2023-01-02 00:00:00')", '-02:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-12-31 23:59:59.999+00:00', 2)", '+06:30')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('0001-01-07 23:59:59.999+08:00', 1)", '-00:45')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-01-01 00:00:00.000001+08:00', 2)", '+08:15')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-08 23:59:59.999999+08:00', 2)", '-01:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 22:28:18', '2023-07-03 00:00:00+08:00')", '+13:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00', 2)", '-09:45')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00', 2, '2023-07-03')", '+05:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 22:28:18', 2, '2023-07-03 00:00:00+08:00')", '-04:15')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('0001-01-08 12:00:00+08:00')", '+12:00')
    validateTimestamptzCeilFloor("SELECT WEEK_CEIL('2023-07-13 23:59:59+00:00', 2, '2023-07-03 00:00:00+08:00')", '-07:30')

    // 85. WEEKDAY function tests
    qt_weekday_1 """SELECT WEEKDAY('2023-10-09')"""
    qt_weekday_2 """SELECT WEEKDAY('2023-10-15 18:30:00')"""
    qt_weekday_3 """SELECT WEEKDAY(NULL)"""

    // 86. WEEK_FLOOR function tests
    qt_week_floor_1 """SELECT WEEK_FLOOR(cast('2023-07-13 22:28:18' as datetime)) AS result"""
    qt_week_floor_2 """SELECT WEEK_FLOOR('2023-07-13 22:28:18', 2) AS result"""
    qt_week_floor_3 """SELECT WEEK_FLOOR('2023-07-10 22:28:18', 2) AS result"""
    qt_week_floor_4 """SELECT WEEK_FLOOR(cast('2023-07-13' as date)) AS result"""
    qt_week_floor_5 """SELECT WEEK_FLOOR('2023-07-13', 1, '2023-07-03') AS result"""
    qt_week_floor_6 """SELECT WEEK_FLOOR('2023-07-10', 1, '2023-07-10') AS result"""
    qt_week_floor_7 """SELECT WEEK_FLOOR('2023-07-10', 1, '2023-07-10 12:00:00') AS result"""
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00')", '+11:45')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-31 20:00:00+00:00', '2023-07-03 00:00:00+08:00')", '-10:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00', 2, '0001-01-01 00:00:00')", '+04:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('9999-06-15 10:30:45+05:00', 2, '9999-06-07 00:00:00')", '-03:45')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-01-07 23:59:59.999+08:00', 2)", '+09:15')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-06-30 23:59:59.999+08:00', 2, '2023-01-02 00:00:00')", '-02:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-12-31 23:59:59.999+00:00', 2)", '+06:45')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('0001-01-07 23:59:59.999+08:00', 1)", '-00:15')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('9999-12-31 12:00:00.123+08:00', 1)", '+08:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-01-01 00:00:00.000001+08:00', 2)", '-01:15')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-08 23:59:59.999999+08:00', 2)", '+13:15')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 22:28:18', '2023-07-03 00:00:00+08:00')", '-09:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00', 2)", '+05:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00', 2, '2023-07-03')", '-04:45')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 22:28:18', 2, '2023-07-03 00:00:00+08:00')", '+12:30')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('0001-01-08 12:00:00+08:00')", '-07:45')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('9999-12-27 12:00:00+08:00')", '+02:15')
    validateTimestamptzCeilFloor("SELECT WEEK_FLOOR('2023-07-13 23:59:59+00:00', 2, '2023-07-03 00:00:00+08:00')", '-11:15')

    // 87. WEEK function tests
    qt_week_1 """SELECT WEEK('2020-01-01') AS week_result"""
    qt_week_2 """SELECT WEEK('2020-07-01', 1) AS week_result"""
    qt_week_3 """SELECT WEEK('2023-01-01', 0) AS mode_0, WEEK('2023-01-01', 3) AS mode_3"""
    qt_week_4 """SELECT WEEK('2023-01-01', 7) AS week_result"""
    qt_week_5 """SELECT WEEK('2023-01-01', -1) AS week_result"""
    qt_week_6 """SELECT WEEK('2023-12-31 23:59:59', 3) AS week_result"""
    qt_week_7 """SELECT WEEK('2023-12-31 23:59:59', NULL),WEEK(NULL,3)"""

    // 88. WEEKOFYEAR function tests
    qt_weekofyear_1 """SELECT WEEKOFYEAR('2023-05-01') AS week_20230501"""
    qt_weekofyear_2 """SELECT WEEKOFYEAR('2023-01-02') AS week_20230102"""
    qt_weekofyear_3 """SELECT WEEKOFYEAR('2023-12-25') AS week_20231225"""
    qt_weekofyear_4 """select weekofyear('1023-01-04')"""
    qt_weekofyear_5 """SELECT WEEKOFYEAR('2024-01-01') AS week_20240101"""
    qt_weekofyear_6 """SELECT WEEKOFYEAR(NULL) AS week_null_input"""

    // 89. WEEKS_ADD function tests
    qt_weeks_add_1 """SELECT WEEKS_ADD('2023-10-01 08:30:45', 1) AS add_1_week_datetime"""
    qt_weeks_add_2 """SELECT WEEKS_ADD('2023-10-01 14:20:10', -1) AS subtract_1_week_datetime"""
    qt_weeks_add_3 """SELECT WEEKS_ADD('2023-05-20', 2) AS add_2_week_date"""
    qt_weeks_add_4 """SELECT WEEKS_ADD('2023-12-25', 1) AS cross_year_add"""
    qt_weeks_add_5 """SELECT WEEKS_ADD(NULL, 5) AS null_input"""
    qt_weeks_add_6 """SELECT WEEKS_ADD('2023-12-31 23:59:59+08:00', 1)"""
    qt_weeks_add_7 """SELECT WEEKS_ADD('2023-01-01 00:00:00Z', 2)"""
    qt_weeks_add_8 """SELECT WEEKS_ADD('2024-02-29 12:30:45.123-05:00', 1)"""
    qt_weeks_add_9 """SELECT WEEKS_ADD('0001-01-01 00:00:00+00:00', 4)"""
    qt_weeks_add_10 """SELECT WEEKS_ADD('2023-06-15 15:45:30.555+09:00', -2)"""
    qt_weeks_add_11 """SELECT WEEKS_ADD('9999-12-31 12:00:00-12:00', 1)"""

    // 90. WEEKS_DIFF function tests
    qt_weeks_diff_1 """SELECT WEEKS_DIFF('2020-12-25', '2020-10-25') AS diff_date"""
    qt_weeks_diff_2 """SELECT WEEKS_DIFF('2020-12-25 10:10:02', '2020-10-25 12:10:02') AS diff_datetime"""
    qt_weeks_diff_3 """SELECT WEEKS_DIFF('2020-12-25 10:10:02', '2020-10-25') AS diff_mixed"""
    qt_weeks_diff_4 """SELECT WEEKS_DIFF('2023-10-07', '2023-10-01') AS diff_6_days"""
    qt_weeks_diff_5 """SELECT WEEKS_DIFF('2023-10-09', '2023-10-01') AS diff_8_days"""
    qt_weeks_diff_6 """SELECT WEEKS_DIFF('2023-10-08 12:00:00', '2023-10-01 00:00:00') AS diff_7_5d, WEEKS_DIFF('2023-10-08 00:00:00', '2023-10-01 12:00:00') AS diff_6_5d"""
    qt_weeks_diff_7 """SELECT WEEKS_DIFF('2023-10-01', '2023-10-08') AS diff_negative"""
    qt_weeks_diff_8 """SELECT WEEKS_DIFF('2024-01-01', '2023-12-25') AS cross_year"""
    qt_weeks_diff_9 """SELECT WEEKS_DIFF(NULL, '2023-10-01') AS null_input1, WEEKS_DIFF('2023-10-01', NULL) AS null_input2"""

    // 91. WEEKS_SUB function tests
    qt_weeks_sub_1 """SELECT WEEKS_SUB('2023-10-01 08:30:45', 1) AS sub_1_week_datetime"""
    qt_weeks_sub_2 """SELECT WEEKS_SUB('2023-09-24 14:20:10', -1) AS add_1_week_datetime"""
    qt_weeks_sub_3 """SELECT WEEKS_SUB('2023-06-03', 2) AS sub_2_week_date"""
    qt_weeks_sub_4 """SELECT WEEKS_SUB('2024-01-01', 1) AS cross_year_sub"""
    qt_weeks_sub_5 """SELECT WEEKS_SUB(NULL, 5) AS null_input"""
    qt_weeks_sub_6 """SELECT WEEKS_SUB('2023-12-31 23:59:59+08:00', 1) AS sub_1_week_tz"""
    qt_weeks_sub_7 """SELECT WEEKS_SUB('2023-01-01 00:00:00Z', 2) AS sub_2_week_utc"""
    qt_weeks_sub_8 """SELECT WEEKS_SUB('2024-02-29 12:30:45.123-05:00', 1) AS leap_year_tz"""
    qt_weeks_sub_9 """SELECT WEEKS_SUB('0001-01-01 00:00:00+00:00', 4) AS ancient_tz"""
    qt_weeks_sub_10 """SELECT WEEKS_SUB('2023-06-15 15:45:30.555+09:00', -2) AS add_2_week_tz"""
    qt_weeks_sub_11 """SELECT WEEKS_SUB('9999-12-31 12:00:00-12:00', 1) AS future_tz"""

    // Group 7: YEAR_CEIL, YEAR_FLOOR, YEAR, YEAR_OF_WEEK, YEARS_ADD, YEARS_DIFF, YEARS_SUB, YEARWEEK (序号92-99)
    
    // 92. YEAR_CEIL function tests
    qt_year_ceil_1 """SELECT YEAR_CEIL('2023-07-13 22:28:18') AS result"""
    qt_year_ceil_2 """SELECT YEAR_CEIL('2023-07-13 22:28:18', 5) AS result"""
    qt_year_ceil_3 """SELECT YEAR_CEIL(cast('2023-07-13' as date)) AS result"""
    qt_year_ceil_4 """SELECT YEAR_CEIL('2023-07-13', 1, '2020-01-01') AS result"""
    qt_year_ceil_5 """SELECT YEAR_CEIL('2023-07-13', 1, '2020-01-01 08:30:00') AS result"""
    qt_year_ceil_6 """SELECT YEAR_CEIL('2023-01-01', 1, '2023-01-01') AS result"""
    qt_year_ceil_7 """SELECT YEAR_CEIL(NULL, 1) AS result"""
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2025-12-31 23:59:59+05:00')", '+10:15')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')", '-08:45')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')", '+03:30')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-07-13 22:28:18+05:00', 5)", '-02:00')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-07-13 22:28:18+05:00', 2, '2020-01-01')", '+07:15')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-07-13 22:28:18', 2, '2020-01-01 00:00:00+08:00')", '-12:00')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('0001-06-15 12:30:00+08:00')", '+01:30')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2025-12-31 20:00:00+00:00', 3, '2020-01-01 00:00:00+08:00')", '-11:30')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-07-13 22:28:18+05:00', 5, '0001-01-01 00:00:00')", '+05:45')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-07-13 22:28:18', 3, '0001-06-15 00:00:00+08:00')", '-05:00')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('9998-06-15 10:30:45+05:00', 1, '9998-01-01 00:00:00')", '+11:00')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('9998-10-20 10:30:45', 1, '9998-01-01 00:00:00+08:00')", '-06:00')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-12-31 23:59:59.999+08:00', 2)", '+12:15')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-12-31 23:59:59.999+00:00', 3, '2020-01-01 00:00:00')", '-01:00')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('0001-12-31 23:59:59.999+08:00', 1)", '+10:30')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-01-01 00:00:00.000001+08:00', 2)", '-09:30')
    validateTimestamptzCeilFloor("SELECT YEAR_CEIL('2023-12-31 23:59:59.999999+08:00', 2)", '+04:45')

    // 93. YEAR_FLOOR function tests
    qt_year_floor_1 """SELECT YEAR_FLOOR('2023-07-13 22:28:18') AS result"""
    qt_year_floor_2 """SELECT YEAR_FLOOR('2023-07-13 22:28:18', 5) AS result"""
    qt_year_floor_3 """SELECT YEAR_FLOOR(cast('2023-07-13' as date)) AS result"""
    qt_year_floor_4 """SELECT YEAR_FLOOR('2023-07-13', 1, '2020-01-01') AS result"""
    qt_year_floor_5 """SELECT YEAR_FLOOR('2023-07-13', 1, '2020-01-01 08:30:00') AS result"""
    qt_year_floor_6 """SELECT YEAR_FLOOR('2023-01-01', 1, '2023-01-01') AS result"""
    qt_year_floor_7 """SELECT YEAR_FLOOR('2019-07-13', 1, '2020-01-01') AS result"""
    qt_year_floor_8 """SELECT YEAR_FLOOR('2025-07-13', 3, '2020-01-01') AS result"""
    qt_year_floor_9 """SELECT YEAR_FLOOR('2023-07-13 06:00:00', 1, '2020-01-01 08:30:00') AS result"""
    qt_year_floor_10 """SELECT YEAR_FLOOR('2023-07-13 10:00:00', 1, '2020-01-01 08:30:00') AS result"""
    qt_year_floor_11 """SELECT YEAR_FLOOR(NULL, 1) AS result"""
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2025-12-31 23:59:59+05:00')", '+10:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')", '-08:30')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')", '+03:15')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-07-13 22:28:18+05:00', 5)", '-02:30')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-07-13 22:28:18+05:00', 2, '2020-01-01')", '+07:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-07-13 22:28:18', 2, '2020-01-01 00:00:00+08:00')", '-11:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('0001-06-15 12:30:00+08:00')", '+01:15')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('9999-06-15 12:30:00+08:00')", '-11:30')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2025-12-31 20:00:00+00:00', 3, '2020-01-01 00:00:00+08:00')", '+05:15')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-07-13 22:28:18+05:00', 5, '0001-01-01 00:00:00')", '-05:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-07-13 22:28:18', 3, '0001-06-15 00:00:00+08:00')", '+11:30')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('9999-06-15 10:30:45+05:00', 1, '9999-01-01 00:00:00')", '-06:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('9999-10-20 10:30:45', 1, '9999-01-01 00:00:00+08:00')", '+12:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-12-31 23:59:59.999+08:00', 2)", '-01:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-12-31 23:59:59.999+00:00', 3, '2020-01-01 00:00:00')", '+09:45')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('0001-12-31 23:59:59.999+08:00', 1)", '-10:15')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('9999-12-31 12:00:00.123+08:00', 1)", '+04:15')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-01-01 00:00:00.000001+08:00', 2)", '-03:30')
    validateTimestamptzCeilFloor("SELECT YEAR_FLOOR('2023-12-31 23:59:59.999999+08:00', 2)", '+06:30')

    // 94. YEAR function tests
    qt_year_1 """SELECT YEAR('1987-01-01') AS year_date"""
    qt_year_2 """SELECT YEAR('2024-05-20 14:30:25') AS year_datetime"""
    qt_year_3 """SELECT YEAR('2023-02-30') AS invalid_date"""
    qt_year_4 """SELECT YEAR(NULL) AS null_input"""

    // 95. YEAR_OF_WEEK function tests
    qt_year_of_week_1 """SELECT YEAR_OF_WEEK('2005-01-01') AS yow_result"""
    qt_year_of_week_2 """SELECT YOW('2005-01-01') AS yow_alias_result"""
    qt_year_of_week_3 """SELECT YEAR_OF_WEEK('2005-01-03') AS yow_result"""
    qt_year_of_week_4 """SELECT YEAR_OF_WEEK('2023-01-01') AS yow_result"""
    qt_year_of_week_5 """SELECT YEAR_OF_WEEK('2023-01-02') AS yow_result"""
    qt_year_of_week_6 """SELECT YEAR_OF_WEEK('2005-01-01 15:30:45') AS yow_datetime"""
    qt_year_of_week_7 """SELECT YEAR_OF_WEEK('2024-12-30') AS yow_result"""
    qt_year_of_week_8 """SELECT YEAR_OF_WEEK(NULL) AS yow_null"""

    // 96. YEARS_ADD function tests
    qt_years_add_1 """SELECT YEARS_ADD('2020-01-31 02:02:02', 1) AS add_1_year_datetime"""
    qt_years_add_2 """SELECT YEARS_ADD('2023-05-10 15:40:20', -1) AS subtract_1_year_datetime"""
    qt_years_add_3 """SELECT YEARS_ADD('2019-12-25', 3) AS add_3_year_date"""
    qt_years_add_4 """SELECT YEARS_ADD('2020-02-29', 1) AS leap_day_adjust"""
    qt_years_add_5 """SELECT YEARS_ADD('2023-01-31', 1) AS month_day_adjust"""
    qt_years_add_6 """SELECT YEARS_ADD(NULL, 5) AS null_input"""
    qt_years_add_7 """SELECT YEARS_ADD('2023-12-31 23:59:59+08:00', 1)"""
    qt_years_add_8 """SELECT YEARS_ADD('2023-01-01 00:00:00Z', 2)"""
    qt_years_add_9 """SELECT YEARS_ADD('2024-02-29 12:30:45.123-05:00', 1)"""
    qt_years_add_10 """SELECT YEARS_ADD('0001-01-01 00:00:00+00:00', 10)"""
    qt_years_add_11 """SELECT YEARS_ADD('2023-06-15 15:45:30.555+09:00', -3)"""
    qt_years_add_12 """SELECT YEARS_ADD('9999-12-31 12:00:00-12:00', -1)"""

    // 97. YEARS_DIFF function tests
    qt_years_diff_1 """SELECT YEARS_DIFF('2020-12-25', '2019-12-25') AS diff_full_year"""
    qt_years_diff_2 """SELECT YEARS_DIFF('2020-11-25', '2019-12-25') AS diff_less_than_year"""
    qt_years_diff_3 """SELECT YEARS_DIFF('2022-03-15 08:30:00', '2021-03-15 09:10:00') AS diff_datetime"""
    qt_years_diff_4 """SELECT YEARS_DIFF('2024-05-20', '2020-05-20 12:00:00') AS diff_mixed"""
    qt_years_diff_5 """SELECT YEARS_DIFF('2018-06-10', '2020-06-10') AS diff_negative"""
    qt_years_diff_6 """SELECT YEARS_DIFF('2024-02-29', '2023-02-28') AS leap_year_diff"""
    qt_years_diff_7 """SELECT YEARS_DIFF(NULL, '2023-03-15') AS null_input1, YEARS_DIFF('2023-03-15', NULL) AS null_input2"""

    // 98. YEARS_SUB function tests
    qt_years_sub_1 """SELECT YEARS_SUB('2020-02-02 02:02:02', 1) AS sub_1_year_datetime"""
    qt_years_sub_2 """SELECT YEARS_SUB('2022-05-10 15:40:20', -1) AS add_1_year_datetime"""
    qt_years_sub_3 """SELECT YEARS_SUB('2022-12-25', 3) AS sub_3_year_date"""
    qt_years_sub_4 """SELECT YEARS_SUB('2020-02-29', 1) AS leap_day_adjust_1"""
    qt_years_sub_5 """SELECT YEARS_SUB(NULL, 5) AS null_input"""
    qt_years_sub_6 """SELECT YEARS_SUB('2023-12-31 23:59:59+08:00', 1)"""
    qt_years_sub_7 """SELECT YEARS_SUB('2023-01-01 00:00:00Z', 2)"""
    qt_years_sub_8 """SELECT YEARS_SUB('2024-02-29 12:30:45.123-05:00', 1)"""
    qt_years_sub_9 """SELECT YEARS_SUB('9999-01-01 00:00:00+00:00', 10)"""
    qt_years_sub_10 """SELECT YEARS_SUB('2023-06-15 15:45:30.555+09:00', -3)"""
    qt_years_sub_11 """SELECT YEARS_SUB('0001-12-31 12:00:00-12:00', -1)"""

    // 99. YEARWEEK function tests
    qt_yearweek_1 """SELECT YEARWEEK('2021-01-01') AS yearweek_mode0"""
    qt_yearweek_2 """SELECT YEARWEEK('2020-07-01', 1) AS yearweek_mode1"""
    qt_yearweek_3 """SELECT YEARWEEK('2024-12-30', 1) AS cross_year_mode1"""
    qt_yearweek_4 """SELECT YEARWEEK('2023-01-02', 5) AS yearweek_mode5"""
    qt_yearweek_5 """SELECT YEARWEEK('2023-12-25', 1) AS date_type_mode1"""

    // 100. MAKETIME function test;
    sql """DROP TABLE IF EXISTS maketime_test"""
    sql """CREATE TABLE maketime_test (
            `id` INT,
            `hour` INT,
            `minute` INT,
            `sec` FLOAT
        ) DUPLICATE KEY(id)
        PROPERTIES ( 'replication_num' = '1' );"""
    sql """ INSERT INTO maketime_test VALUES
                (1, 12, 15, 30),
                (2, 111, 0, 23.1234567),
                (3, 1234, 11, 4),
                (4, -1234, 6, 52),
                (5, 20, 60, 12),
                (6, 14, 51, 66),
                (7, NULL, 15, 16),
                (8, 7, NULL, 8),
                (9, 1, 2, NULL),
                (10, 123, -4, 52),
                (11, 7, 23, -6);"""
    qt_maketime_test_1 """SELECT MAKETIME(hour,minute,sec) FROM maketime_test ORDER BY id;"""
    qt_maketime_test_2 """SELECT MAKETIME(hour, minute, 25) FROM maketime_test ORDER BY id;"""

    //102. TIMESTAMP with two args Function test
    sql """DROP TABLE IF EXISTS test_timestamp"""
    sql """CREATE TABLE test_timestamp (
            id INT,
            dt DATE,
            dttm DATETIME,
        ) PROPERTIES ( 'replication_num' = '1' );"""
    sql """INSERT INTO test_timestamp VALUES
            (1, '2025-11-01', '2025-11-01 12:13:14'),
            (2, '2025-1-1', '2025-1-1 23:59:59'),
            (3, '2025-1-31', '2025-1-31 23:59:59'),
            (4, '2025-12-31', '2025-12-31 23:59:59'),
            (5, NULL, NULL);"""
    qt_timestamp_two_args_1 """ SELECT 
                                    TIMESTAMP(dt, '65:43:21') AS date_cross_day,
                                    TIMESTAMP(dttm, '1:23:45') AS dttm_cross_day,
                                    TIMESTAMP(dt, NULL) AS all_null_1,
                                    TIMESTAMP(dttm, NULL) AS all_null_2
                                FROM test_timestamp;"""
    qt_timestamp_two_args_2 """SELECT TIMESTAMP('12:13:14', '11:45:14');"""
    qt_timestamp_two_args_3 """SELECT TIMESTAMP('2026-01-05 11:45:14+05:30', '02:15:30');"""

    explain {
        sql """SELECT TIMESTAMP(dt, '65:43:21') FROM test_timestamp;"""
        contains "add_time"
    }
    
    test {
        sql """SELECT TIMESTAMP('9999-12-31', '65:43:21');"""
        exception "is invalid";
    }
    

    // Test constant folding for YEARWEEK function
    testFoldConst("SELECT YEARWEEK('2021-01-01') AS yearweek_mode0")
    testFoldConst("SELECT YEARWEEK('2020-07-01', 1) AS yearweek_mode1")
    testFoldConst("SELECT YEARWEEK('2024-12-30', 1) AS cross_year_mode1")
    testFoldConst("SELECT YEARWEEK('2023-01-02', 5) AS yearweek_mode5")
    testFoldConst("SELECT YEARWEEK('2023-12-25', 1) AS date_type_mode1")

    //101. TIME_FORMAT function tests
    sql """ DROP TABLE IF EXISTS test_time_format; """
    sql """CREATE TABLE test_time_format (
        id  INT,
        tm VARCHAR(32)
    ) DUPLICATE KEY(id)
    PROPERTIES ( 'replication_num' = '1' );
        """
    sql """ INSERT INTO test_time_format VALUES
            ( 1, '00:00:00'),
            ( 2, '00:00:00.123456'),
            ( 3, '12:34:56'),
            ( 4, '12:34:56.789012'),
            ( 5, '23:59:59'),
            ( 6, '23:59:59.999999'),
            ( 7, '08:00:00'),
            ( 8, '15:00:00'),
            ( 9, '100:00:00'),
            (10, '123:45:56'),
            (11, '838:59:59.999999'),
            (12, '-00:00:01'),
            (13, '-12:34:56.000001'),
            (14, '-838:59:59.999999'),
            (15, NULL);
        """
    qt_time_format_1 """SELECT
                            id,
                            tm,
                            TIME_FORMAT(tm, '%H'),
                            TIME_FORMAT(tm, '%k'),
                            TIME_FORMAT(tm, '%h'),
                            TIME_FORMAT(tm, '%I'),
                            TIME_FORMAT(tm, '%l'),
                            TIME_FORMAT(tm, '%i'),
                            TIME_FORMAT(tm, '%s'),
                            TIME_FORMAT(tm, '%S'),
                            TIME_FORMAT(tm, '%f'),
                            TIME_FORMAT(tm, '%p'),
                            TIME_FORMAT(tm, '%r'),
                            TIME_FORMAT(tm, '%T'),
                            TIME_FORMAT(tm, '%H:%i:%s.%f'),
                            TIME_FORMAT(tm, '%k %H %l %I %h'),
                            TIME_FORMAT(tm, '%s %f %i %p'),
                            TIME_FORMAT(tm, '%T %r %h:%I'),
                            TIME_FORMAT(tm, '%l %k %I %H %h %p'),
                            TIME_FORMAT(tm, '%f %s %i %T %r')
                        FROM test_time_format
                        ORDER BY id;
                        """
    qt_time_format_2 """SELECT TIME_FORMAT('2023-01-01 00:00:00', '%H %k %l %I %h %p')"""
    qt_time_format_3 """SELECT TIME_FORMAT('2023-01-01 00:00:00.123456', '%s %f %i %T')"""
    qt_time_format_4 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%r %T %h:%I %l')"""
    qt_time_format_5 """SELECT TIME_FORMAT('2023-01-01 12:34:56.789012', '%k %H %I %l %h %p %s')"""
    qt_time_format_6 """SELECT TIME_FORMAT('2023-01-01 23:59:59', '%f %s %i %p %r')"""
    qt_time_format_7 """SELECT TIME_FORMAT('2023-01-01 23:59:59.999999', '%T %r %H:%i:%s.%f')"""
    qt_time_format_8 """SELECT TIME_FORMAT('2023-01-01 08:00:00', '%l %k %h %I %H %p %f')"""
    qt_time_format_9 """SELECT TIME_FORMAT('2023-01-01 15:00:00', '%s %i %f %T %r %p')"""
    qt_time_format_10 """SELECT TIME_FORMAT('2023-01-01 100:00:00', '%H %l %I %k %h %s %f')"""
    qt_time_format_11 """SELECT TIME_FORMAT('2023-01-01 123:45:56', '%p %r %T %i %s %f %H')"""
    qt_time_format_12 """SELECT TIME_FORMAT('2023-01-01 838:59:59.999999', '%k %f %s %I %l %H %p')"""
    qt_time_format_13 """SELECT TIME_FORMAT('2023-01-01 00:00:01', '%T %i %r %s %f %h:%I')"""
    qt_time_format_14 """SELECT TIME_FORMAT('2023-01-01 12:34:56.000001', '%p %H %k %l %I %T %f')"""
    qt_time_format_15 """SELECT TIME_FORMAT('2023-01-01 838:59:59.999999', '%s %i %f %r %p %H:%i:%s')"""

    // Time format with date placeholders (Year, Month, Day return zeros or NULL)
    qt_time_format_16 """SELECT TIME_FORMAT('2023-01-01 12:34:56.789012', '%Y-%m-%d %H:%i:%s')"""
    qt_time_format_17 """SELECT TIME_FORMAT('2023-01-01 01:02:03.456789', '%y-%m-%d')"""
    qt_time_format_18 """SELECT TIME_FORMAT('2023-01-01 23:59:59.999999', '%Y %m %d')"""
    qt_time_format_19 """SELECT TIME_FORMAT('2023-01-01 00:00:00', '%c-%e')"""
    qt_time_format_20 """SELECT TIME_FORMAT('2023-01-01 15:45:30.123456', '%Y/%m/%d %H:%i:%s.%f')"""
    qt_time_format_21 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%M')"""
    qt_time_format_22 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%W')"""
    qt_time_format_23 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%j')"""
    qt_time_format_24 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%D')"""
    qt_time_format_25 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%U')"""
    qt_time_format_26 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%u')"""
    qt_time_format_27 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%V')"""
    qt_time_format_28 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%v')"""
    qt_time_format_29 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%x')"""
    qt_time_format_30 """SELECT TIME_FORMAT('2023-01-01 12:34:56', '%X %w')"""

    // TO_SECONDS function tests
    qt_to_seconds_1 """select to_seconds('2007-10-07')"""
    qt_to_seconds_2 """select to_seconds('2007-10-07 10:03:09')"""
    qt_to_seconds_3 """select to_seconds('0000-01-01 00:00:00')"""
    qt_to_seconds_4 """select to_seconds('9999-12-31 23:59:59')"""
    qt_to_seconds_5 """select to_seconds('101-01-01 08:30:15.123456')"""
    qt_to_seconds_6 """select to_seconds('2023-02-30 12:00:00')"""
    qt_to_seconds_7 """select to_seconds(NULL)"""
    qt_to_seconds_8 """select to_seconds('2023-10-07 12:34:56.654321')"""
    qt_to_seconds_9 """select to_seconds('12:34:56')"""
    qt_to_seconds_10 """select to_seconds('-12:34:56.123456')"""
    qt_to_seconds_11 """select to_seconds(20250101)"""
    qt_to_seconds_12 """select to_seconds(20250101123045)"""
    testFoldConst("SELECT to_seconds('2007-10-07')")
    testFoldConst("SELECT to_seconds('2007-10-07 10:03:09')")
    testFoldConst("SELECT to_seconds('0000-01-01 00:00:00')")
    testFoldConst("SELECT to_seconds('9999-12-31 23:59:59')")
    testFoldConst("SELECT to_seconds('101-01-01 08:30:15.123456')")
    testFoldConst("SELECT to_seconds('2023-02-30 12:00:00')")
    testFoldConst("SELECT to_seconds(NULL)")
    testFoldConst("SELECT to_seconds('2023-10-07 12:34:56.654321')")
    testFoldConst("SELECT to_seconds('12:34:56')")
    testFoldConst("SELECT to_seconds('-12:34:56.123456')")
    testFoldConst("SELECT to_seconds(20250101)")
    testFoldConst("SELECT to_seconds(20250101123045)")

    // Test constant folding for Group 1 functions
    
    // 1. CONVERT_TZ function constant folding tests
    testFoldConst("SELECT CONVERT_TZ(CAST('2019-08-01 13:21:03' AS DATETIME), 'Asia/Shanghai', 'America/Los_Angeles')")
    testFoldConst("SELECT CONVERT_TZ(CAST('2019-08-01 13:21:03' AS DATETIME), '+08:00', 'America/Los_Angeles')")
    testFoldConst("SELECT CONVERT_TZ(CAST('2019-08-01 13:21:03' AS DATE), 'Asia/Shanghai', 'America/Los_Angeles')")
    // testFoldConst("SELECT CONVERT_TZ('2038-01-19 03:14:07', 'GMTaa', 'MET')")
    testFoldConst("SELECT CONVERT_TZ('2019-08-01 13:21:03.636', '+08:00', 'America/Los_Angeles')")
    testFoldConst("SELECT CONVERT_TZ(NULL, 'Asia/Shanghai', 'America/Los_Angeles')")
    testFoldConst("SELECT CONVERT_TZ('2019-08-01 13:21:03', NULL, 'America/Los_Angeles')")
    testFoldConst("SELECT CONVERT_TZ('2019-08-01 13:21:03', 'Asia/Shanghai', NULL)")
    // testFoldConst("SELECT CONVERT_TZ('2019-08-01 13:21:03', 'Invalid/Timezone', 'America/Los_Angeles')")
    // testFoldConst("SELECT CONVERT_TZ('2019-08-01 13:21:03', 'Asia/Shanghai', 'Invalid/Timezone')")

    // 2. DATE_ADD function constant folding tests
    testFoldConst("SELECT DATE_ADD(CAST('2010-11-30 23:59:59' AS DATETIME), INTERVAL 2 DAY)")
    testFoldConst("SELECT DATE_ADD(CAST('2023-01-01' AS DATE), INTERVAL 1 QUARTER)")
    testFoldConst("SELECT DATE_ADD('2023-01-01', INTERVAL 1 WEEK)")
    testFoldConst("SELECT DATE_ADD('2023-01-31', INTERVAL 1 MONTH)")
    testFoldConst("SELECT DATE_ADD('2019-01-01', INTERVAL -3 DAY)")
    testFoldConst("SELECT DATE_ADD('2023-12-31 23:00:00', INTERVAL 2 HOUR)")
    testFoldConst("SELECT DATE_ADD(NULL, INTERVAL 1 DAY)")
    testFoldConst("SELECT DATE_ADD('2023-12-31 23:59:59+08:00', INTERVAL 1 YEAR)")
    testFoldConst("SELECT DATE_ADD('2023-06-15 12:30:45-05:00', INTERVAL 2 QUARTER)")
    testFoldConst("SELECT DATE_ADD('2023-01-31 10:15:30+00:00', INTERVAL 3 MONTH)")
    testFoldConst("SELECT DATE_ADD('2024-02-29 15:45:22+09:00', INTERVAL 1 WEEK)")
    testFoldConst("SELECT DATE_ADD('2023-12-25 08:00:00-08:00', INTERVAL 5 DAY)")
    testFoldConst("SELECT DATE_ADD('2023-01-01 00:00:00Z', INTERVAL 10 HOUR)")
    testFoldConst("SELECT DATE_ADD('2023-06-15 23:59:59+05:30', INTERVAL 45 MINUTE)")
    testFoldConst("SELECT DATE_ADD('2023-12-31 23:59:59.999999+08:00', INTERVAL 1 SECOND)")
    testFoldConst("SELECT DATE_ADD('0001-01-01 00:00:00Z', INTERVAL '1 5:30:45' DAY_SECOND)")
    testFoldConst("SELECT DATE_ADD('9999-12-28 12:00:00+03:00', INTERVAL '2 10' DAY_HOUR)")
    testFoldConst("SELECT DATE_ADD('2023-06-15 15:45:30-07:00', INTERVAL '30:45' MINUTE_SECOND)")
    testFoldConst("SELECT DATE_ADD('2024-01-15 10:20:30.123456+01:00', INTERVAL '2.500000' SECOND_MICROSECOND)")
    testFoldConst("SELECT DATE_ADD('2023-03-15 14:30:00+11:00', INTERVAL -1 YEAR)")
    testFoldConst("SELECT DATE_ADD('2024-12-31 23:59:59-12:00', INTERVAL -5 DAY)")
    testFoldConst("SELECT DATE_ADD('2023-07-04 16:20:15+00:00', INTERVAL -2 HOUR)")
    testFoldConst("SELECT DATE_ADD('2025-02-28 18:45:22+06:00', INTERVAL -30 MINUTE)")
    testFoldConst("SELECT DATE_ADD('2023-11-11 11:11:11.111111-03:00', INTERVAL -1 SECOND)")
    testFoldConst("SELECT date_add('2023-02-28 16:00:00 UTC', INTERVAL 1 DAY)")
    testFoldConst("SELECT date_add('2023-02-28 16:00:00 America/New_York', INTERVAL 1 DAY)")
    testFoldConst("SELECT date_add('2023-02-28 16:00:00UTC', INTERVAL 1 DAY)")
    testFoldConst("SELECT date_add('2023-02-28 16:00:00America/New_York', INTERVAL 1 DAY)")
    testFoldConst("SELECT date_add('2023-03-12 01:30:00 Europe/London', INTERVAL 1 DAY)")
    testFoldConst("SELECT date_add('2023-11-05 01:30:00 America/New_York', INTERVAL 1 DAY)")

    // 3. DATE_CEIL function constant folding tests
    testFoldConst("SELECT DATE_CEIL(CAST('2023-07-13 22:28:18' AS DATETIME), INTERVAL 5 SECOND)")
    testFoldConst("SELECT DATE_CEIL(CAST('2023-07-13 22:28:18.123' AS DATETIME(3)), INTERVAL 5 SECOND)")
    testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL 5 MINUTE)")
    testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL 5 WEEK)")
    testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL 5 HOUR)")
    testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL 5 DAY)")
    testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL 5 MONTH)")
    testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL 5 YEAR)")
    testFoldConst("SELECT DATE_CEIL(NULL, INTERVAL 5 SECOND)")
    // testFoldConst("SELECT DATE_CEIL('2023-07-13 22:28:18', INTERVAL -5 SECOND)")
    testFoldConst("SELECT DATE_CEIL('2023-03-15 14:25:38.999999+02:00', INTERVAL 1 SECOND)")
    testFoldConst("SELECT DATE_CEIL('2024-06-20 23:59:59.123456-04:00', INTERVAL 5 SECOND)")
    testFoldConst("SELECT DATE_CEIL('2024-06-20 09:59:45+05:00', INTERVAL 3 MINUTE)")
    testFoldConst("SELECT DATE_CEIL('2023-11-08 23:58:30+09:00', INTERVAL 5 MINUTE)")
    testFoldConst("SELECT DATE_CEIL('2023-11-08 23:35:12-05:00', INTERVAL 2 HOUR)")
    testFoldConst("SELECT DATE_CEIL('2024-02-29 22:45:30Z', INTERVAL 3 HOUR)")
    testFoldConst("SELECT DATE_CEIL('2024-01-31 20:18:45+01:00', INTERVAL 2 DAY)")
    testFoldConst("SELECT DATE_CEIL('2023-12-30 22:30:15+05:30', INTERVAL 3 DAY)")
    testFoldConst("SELECT DATE_CEIL('2023-09-10 23:22:33-07:00', INTERVAL 2 WEEK)")
    testFoldConst("SELECT DATE_CEIL('2024-12-29 20:15:40+03:00', INTERVAL 1 WEEK)")
    testFoldConst("SELECT DATE_CEIL('2024-01-31 22:41:56-06:00', INTERVAL 2 MONTH)")
    testFoldConst("SELECT DATE_CEIL('2023-12-15 21:30:00+02:00', INTERVAL 3 MONTH)")
    testFoldConst("SELECT DATE_CEIL('2024-12-31 20:52:27-08:00', INTERVAL 2 YEAR)")
    testFoldConst("SELECT DATE_CEIL('2023-11-15 22:30:15+01:00', INTERVAL 3 YEAR)")

    // 4. DATEDIFF function constant folding tests
    testFoldConst("SELECT DATEDIFF(CAST('2007-12-31 23:59:59' AS DATETIME), CAST('2007-12-30' AS DATETIME))")
    testFoldConst("SELECT DATEDIFF(CAST('2010-11-30 23:59:59' AS DATETIME), CAST('2010-12-31' AS DATETIME))")
    testFoldConst("SELECT DATEDIFF('2023-01-02 13:00:00', '2023-01-01 12:00:00')")
    testFoldConst("SELECT DATEDIFF('2023-01-02 12:00:00', '2023-01-01 13:00:00')")
    testFoldConst("SELECT DATEDIFF(NULL, '2023-01-01')")

    // 5. DATE_FLOOR function constant folding tests
    testFoldConst("SELECT DATE_FLOOR(CAST('0001-01-01 00:00:18' AS DATETIME), INTERVAL 5 SECOND)")
    testFoldConst("SELECT DATE_FLOOR(CAST('0001-01-01 00:00:18.123' AS DATETIME), INTERVAL 5 SECOND)")
    testFoldConst("SELECT DATE_FLOOR('2023-07-10 00:00:00', INTERVAL 5 DAY)")
    testFoldConst("SELECT DATE_FLOOR('2023-07-13', INTERVAL 5 YEAR)")
    testFoldConst("SELECT DATE_FLOOR(NULL, INTERVAL 5 DAY)")
    testFoldConst("SELECT DATE_FLOOR('2023-03-15 14:25:38.999999+02:00', INTERVAL 1 SECOND)")
    testFoldConst("SELECT DATE_FLOOR('2024-06-20 23:59:59.123456-04:00', INTERVAL 5 SECOND)")
    testFoldConst("SELECT DATE_FLOOR('2024-06-20 10:01:45+05:00', INTERVAL 3 MINUTE)")
    testFoldConst("SELECT DATE_FLOOR('2023-11-09 00:02:30+09:00', INTERVAL 5 MINUTE)")
    testFoldConst("SELECT DATE_FLOOR('2023-11-09 01:35:12-05:00', INTERVAL 2 HOUR)")
    testFoldConst("SELECT DATE_FLOOR('2024-03-01 02:45:30Z', INTERVAL 3 HOUR)")
    testFoldConst("SELECT DATE_FLOOR('2024-02-01 20:18:45+01:00', INTERVAL 2 DAY)")
    testFoldConst("SELECT DATE_FLOOR('2024-01-02 22:30:15+05:30', INTERVAL 3 DAY)")
    testFoldConst("SELECT DATE_FLOOR('2023-09-11 23:22:33-07:00', INTERVAL 2 WEEK)")
    testFoldConst("SELECT DATE_FLOOR('2025-01-05 20:15:40+03:00', INTERVAL 1 WEEK)")
    testFoldConst("SELECT DATE_FLOOR('2024-02-29 22:41:56-06:00', INTERVAL 2 MONTH)")
    testFoldConst("SELECT DATE_FLOOR('2024-01-15 21:30:00+02:00', INTERVAL 3 MONTH)")
    testFoldConst("SELECT DATE_FLOOR('2025-01-10 20:52:27-08:00', INTERVAL 2 YEAR)")
    testFoldConst("SELECT DATE_FLOOR('2024-11-15 22:30:15+01:00', INTERVAL 3 YEAR)")

    // 6. DATE_FORMAT function constant folding tests
    testFoldConst("SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')")
    testFoldConst("SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s')")
    testFoldConst("SELECT DATE_FORMAT('1999-01-01', '%Y-%m-%d')")
    testFoldConst("SELECT DATE_FORMAT('1999-01-01 00:00:00', '%d/%m/%Y %H:%i:%s')")
    testFoldConst("SELECT DATE_FORMAT('2009-10-04', '%a %b %c')")
    testFoldConst("SELECT DATE_FORMAT('2009-10-04', '%D %e %f')")
    testFoldConst("SELECT DATE_FORMAT(NULL, '%Y-%m-%d')")
    testFoldConst("SELECT DATE_FORMAT('2009-10-04', NULL)")
    testFoldConst("SELECT DATE_FORMAT('2009-10-04 22:23:00', '   %W %M    %Y')")

    // 7. DATE function constant folding tests
    testFoldConst("SELECT DATE('2003-12-31 01:02:03')")
    testFoldConst("SELECT DATE('2003-12-31')")
    testFoldConst("SELECT DATE(NULL)")

    // 8. DATE_SUB function constant folding tests
    testFoldConst("SELECT DATE_SUB('2018-05-01', INTERVAL 1 DAY)")
    testFoldConst("SELECT DATE_SUB('2018-05-01', INTERVAL 1 MONTH)")
    testFoldConst("SELECT DATE_SUB('2018-05-01', INTERVAL 1 YEAR)")
    testFoldConst("SELECT DATE_SUB('2018-05-01 12:00:00', INTERVAL 2 HOUR)")
    testFoldConst("SELECT DATE_SUB(NULL, INTERVAL 1 DAY)")
    testFoldConst("SELECT DATE_SUB('2023-12-31 23:59:59+08:00', INTERVAL 1 YEAR)")
    testFoldConst("SELECT DATE_SUB('2023-06-15 12:30:45-05:00', INTERVAL 2 QUARTER)")
    testFoldConst("SELECT DATE_SUB('2023-01-31 10:15:30+00:00', INTERVAL 3 MONTH)")
    testFoldConst("SELECT DATE_SUB('2024-02-29 15:45:22+09:00', INTERVAL 1 WEEK)")
    testFoldConst("SELECT DATE_SUB('2023-12-25 08:00:00-08:00', INTERVAL 5 DAY)")
    testFoldConst("SELECT DATE_SUB('2023-01-01 00:00:00Z', INTERVAL 10 HOUR)")
    testFoldConst("SELECT DATE_SUB('2023-06-15 23:59:59+05:30', INTERVAL 45 MINUTE)")
    testFoldConst("SELECT DATE_SUB('2023-12-31 23:59:59.999999+08:00', INTERVAL 1 SECOND)")
    testFoldConst("SELECT DATE_SUB('2023-03-15 14:30:00+11:00', INTERVAL -1 YEAR)")
    testFoldConst("SELECT DATE_SUB('2024-12-31 23:59:59-12:00', INTERVAL -5 DAY)")
    testFoldConst("SELECT DATE_SUB('2023-07-04 16:20:15+00:00', INTERVAL -2 HOUR)")
    testFoldConst("SELECT DATE_SUB('2025-02-28 18:45:22+06:00', INTERVAL -30 MINUTE)")
    testFoldConst("SELECT DATE_SUB('2023-11-11 11:11:11.111111-03:00', INTERVAL -1 SECOND)")

    // 9. DATE_TRUNC function constant folding tests
    testFoldConst("SELECT DATE_TRUNC('2019-05-09', 'year')")
    testFoldConst("SELECT DATE_TRUNC('2019-05-09', 'month')")
    testFoldConst("SELECT DATE_TRUNC('2019-05-09 12:30:45', 'day')")
    testFoldConst("SELECT DATE_TRUNC('2019-05-09 12:30:45', 'hour')")
    testFoldConst("SELECT DATE_TRUNC(NULL, 'day')")
    testFoldConst("SELECT DATE_TRUNC('2023-03-15 14:25:38.999999+02:00', 'second')")
    testFoldConst("SELECT DATE_TRUNC('2024-06-20 09:47:59.123456-04:00', 'second')")
    testFoldConst("SELECT DATE_TRUNC('2024-06-20 09:59:45-04:00', 'minute')")
    testFoldConst("SELECT DATE_TRUNC('2023-11-08 23:59:30+09:00', 'minute')")
    testFoldConst("SELECT DATE_TRUNC('2023-11-08 23:35:12+09:00', 'hour')")
    testFoldConst("SELECT DATE_TRUNC('2024-02-29 23:45:30-05:00', 'hour')")
    testFoldConst("SELECT DATE_TRUNC('2023-05-15 22:30:15+05:30', 'hour')")
    testFoldConst("SELECT DATE_TRUNC('2024-01-31 23:18:45Z', 'day')")
    testFoldConst("SELECT DATE_TRUNC('2023-12-31 20:30:15+02:00', 'day')")
    testFoldConst("SELECT DATE_TRUNC('2024-03-31 22:15:40-06:00', 'day')")
    testFoldConst("SELECT DATE_TRUNC('2023-09-10 23:22:33+05:30', 'week')")
    testFoldConst("SELECT DATE_TRUNC('2024-12-29 22:15:40-05:00', 'week')")
    testFoldConst("SELECT DATE_TRUNC('2024-01-31 23:41:56-07:00', 'month')")
    testFoldConst("SELECT DATE_TRUNC('2023-12-31 20:30:00+01:00', 'month')")
    testFoldConst("SELECT DATE_TRUNC('2024-06-30 23:45:20+05:00', 'month')")
    testFoldConst("SELECT DATE_TRUNC('2023-03-31 23:29:14+01:00', 'quarter')")
    testFoldConst("SELECT DATE_TRUNC('2024-09-30 22:45:30-06:00', 'quarter')")
    testFoldConst("SELECT DATE_TRUNC('2023-12-31 21:15:45+02:00', 'quarter')")
    testFoldConst("SELECT DATE_TRUNC('2024-12-31 23:52:27-11:00', 'year')")
    testFoldConst("SELECT DATE_TRUNC('2025-12-31 22:30:15+03:00', 'year')")
    testFoldConst("SELECT DATE_TRUNC('2023-12-31 20:15:45+01:00', 'year')")

    // Test constant folding for Group 2 functions (Day functions and related)
    
    // 13. DAY_CEIL function constant folding tests
    testFoldConst("SELECT DAY_CEIL(CAST('2023-07-13 22:28:18' AS DATETIME), 5)")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18.123', 5)")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18')")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18', 7, '2023-01-01 00:00:00')")
    testFoldConst("SELECT DAY_CEIL(CAST('2023-07-13' AS DATE), 3)")
    // testFoldConst("SELECT DAY_CEIL('2023-07-13', 0)")
    testFoldConst("SELECT DAY_CEIL(NULL, 5)")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18+05:00')")
    testFoldConst("SELECT DAY_CEIL('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18+05:00', 5)")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18+05:00', 5, '2023-01-01')")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT DAY_CEIL('0001-01-01 12:00:00+08:00')")
    testFoldConst("SELECT DAY_CEIL('9999-12-30 23:59:59+08:00')")
    testFoldConst("SELECT DAY_CEIL('2023-07-13 23:59:59+00:00', 5, '2023-01-01 00:00:00+08:00')")

    // 14. DAY_FLOOR function constant folding tests
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18.123', 5)")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18')")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18', 7, '2023-01-01 00:00:00')")
    testFoldConst("SELECT DAY_FLOOR(CAST('2023-07-13' AS DATE), 3)")
    testFoldConst("SELECT DAY_FLOOR(NULL, 5)")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18+05:00')")
    testFoldConst("SELECT DAY_FLOOR('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18+05:00', 5)")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18+05:00', 5, '2023-01-01')")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT DAY_FLOOR('0001-01-02 12:00:00+08:00')")
    testFoldConst("SELECT DAY_FLOOR('9999-12-31 23:59:59+08:00')")
    testFoldConst("SELECT DAY_FLOOR('2023-07-13 23:59:59+00:00', 5, '2023-01-01 00:00:00+08:00')")

    // 15. DAY function constant folding tests
    testFoldConst("SELECT DAY('1987-01-31')")
    testFoldConst("SELECT DAY('2023-07-13 22:28:18')")
    testFoldConst("SELECT DAY(NULL)")

    // 16. DAYNAME function constant folding tests
    testFoldConst("SELECT DAYNAME('2007-02-03 00:00:00')")
    testFoldConst("SELECT DAYNAME('2023-10-01')")
    testFoldConst("SELECT DAYNAME(NULL)")

    // 17. DAYOFWEEK function constant folding tests
    testFoldConst("SELECT DAYOFWEEK('2019-06-25')")
    testFoldConst("SELECT DAYOFWEEK('2019-06-25 15:30:45')")
    testFoldConst("SELECT DAYOFWEEK('2024-02-18')")
    testFoldConst("SELECT DAYOFWEEK(NULL)")

    // 18. DAYOFYEAR function constant folding tests
    testFoldConst("SELECT DAYOFYEAR('2007-02-03 00:00:00')")
    testFoldConst("SELECT DAYOFYEAR('2023-12-31')")
    testFoldConst("SELECT DAYOFYEAR('2024-12-31')")
    testFoldConst("SELECT DAYOFYEAR(NULL)")

    // 19. EXTRACT function constant folding tests
    testFoldConst("SELECT EXTRACT(year from '2022-09-22 17:01:30') as year, EXTRACT(month from '2022-09-22 17:01:30') as month, EXTRACT(day from '2022-09-22 17:01:30') as day")
    testFoldConst("SELECT EXTRACT(quarter from '2023-05-15') as quarter")
    testFoldConst("SELECT EXTRACT(week from '2024-01-06') as week")
    testFoldConst("SELECT EXTRACT(week from '2024-01-07') as week")
    testFoldConst("SELECT EXTRACT(week from '2024-12-31') as week")

    // 20. FROM_DAYS function constant folding tests
    testFoldConst("SELECT FROM_DAYS(730669)")
    testFoldConst("SELECT FROM_DAYS(365)")
    // testFoldConst("SELECT FROM_DAYS(-10)")
    testFoldConst("SELECT FROM_DAYS(NULL)")

    // 21. FROM_ISO8601_DATE function constant folding tests
    testFoldConst("SELECT FROM_ISO8601_DATE('2023') as year_only")
    testFoldConst("SELECT FROM_ISO8601_DATE('2023-10')")
    testFoldConst("SELECT FROM_ISO8601_DATE('2023-10-05') as full_date")
    testFoldConst("SELECT FROM_ISO8601_DATE('2021-001') as day_1")
    testFoldConst("SELECT FROM_ISO8601_DATE('2021-060') as day_60")
    testFoldConst("SELECT FROM_ISO8601_DATE('2024-366') as day_366")
    testFoldConst("SELECT FROM_ISO8601_DATE('0522-W01-1') as week_1")
    testFoldConst("SELECT FROM_ISO8601_DATE('2023-10-01T12:34:10')")
    testFoldConst("SELECT FROM_ISO8601_DATE('0522-661') as day_661")
    testFoldConst("SELECT FROM_ISO8601_DATE('invalid-date')")
    testFoldConst("SELECT FROM_ISO8601_DATE(NULL)")

    // 22. FROM_MILLISECOND function constant folding tests
    testFoldConst("SELECT FROM_MILLISECOND(1618236845000)")
    testFoldConst("SELECT FROM_MILLISECOND(0)")
    testFoldConst("SELECT FROM_MILLISECOND(NULL)")

    // 23. FROM_SECOND_TIMESTAMP function constant folding tests
    testFoldConst("SELECT FROM_SECOND(1618236845)")
    testFoldConst("SELECT FROM_SECOND(0)")
    testFoldConst("SELECT FROM_SECOND(NULL)")

    // 24. FROM_UNIXTIME function constant folding tests
    testFoldConst("SELECT FROM_UNIXTIME(0)")
    testFoldConst("SELECT FROM_UNIXTIME(1196440219)")
    testFoldConst("SELECT FROM_UNIXTIME(1196440219, 'yyyy-MM-dd HH:mm:ss')")
    testFoldConst("SELECT FROM_UNIXTIME(1196440219, '%Y-%m-%d')")
    testFoldConst("SELECT FROM_UNIXTIME(1196440219, '%Y-%m-%d %H:%i:%s')")
    testFoldConst("SELECT FROM_UNIXTIME(32536799, 'gdaskpdp')")
    testFoldConst("SELECT FROM_UNIXTIME(NULL)")

    // Group 3: Hour functions (序号24-29)
    
    // 24. HOUR_CEIL function constant folding tests
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18')")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18', 5, '2023-07-13 20:00:00')")
    testFoldConst("SELECT HOUR_CEIL(CAST('2023-07-13' AS DATE), 3)")
    // testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18', 0)")
    testFoldConst("SELECT HOUR_CEIL(NULL, 5)")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18.456789+05:00', 4)")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18.456789+05:00', 4, '2023-07-13 08:00:00')")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18.789123', 5, '2023-07-13 18:00:00+08:00')")
    testFoldConst("SELECT HOUR_CEIL('0001-07-13 22:28:18.456789+05:00', 4)")
    testFoldConst("SELECT HOUR_CEIL('9999-12-31 22:28:18.456789+05:00', 4)")
    testFoldConst("SELECT HOUR_CEIL('2023-07-13 23:59:59.999+00:00', 4, '2023-07-13 20:00:00+08:00')")
    testFoldConst("SELECT HOUR_CEIL('2023-12-31 23:30:00.111+00:00', 5)")

    // 25. HOUR_FLOOR function constant folding tests
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 22:28:18')")
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 22:28:18', 5, '2023-07-13 20:00:00')")
    testFoldConst("SELECT HOUR_FLOOR(CAST('2023-07-13' AS DATE), 3)")
    testFoldConst("SELECT HOUR_FLOOR(NULL, 5)")
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 22:28:18.456789+05:00', 4)")
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 22:28:18.456789+05:00', 4, '2023-07-13 08:00:00')")
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 22:28:18.789123', 5, '2023-07-13 18:00:00+08:00')")
    testFoldConst("SELECT HOUR_FLOOR('0001-07-13 22:28:18.456789+05:00', 4)")
    testFoldConst("SELECT HOUR_FLOOR('9999-12-31 22:28:18.456789+05:00', 4)")
    testFoldConst("SELECT HOUR_FLOOR('2023-07-13 23:59:59.999+00:00', 4, '2023-07-13 20:00:00+08:00')")
    testFoldConst("SELECT HOUR_FLOOR('2023-12-31 23:30:00.111+00:00', 5)")

    // 26. HOUR function constant folding tests
    testFoldConst("SELECT HOUR('2018-12-31 23:59:59')")
    testFoldConst("SELECT HOUR('2023-01-01 00:00:00')")
    testFoldConst("SELECT HOUR('2023-10-01 12:30:45')")
    testFoldConst("SELECT HOUR('14:25:30')")
    testFoldConst("SELECT HOUR('2023-07-13')")
    testFoldConst("SELECT HOUR(NULL)")

    // 27. HOURS_ADD function constant folding tests
    testFoldConst("SELECT HOURS_ADD('2020-02-02 02:02:02', 1)")
    testFoldConst("SELECT HOURS_ADD('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT HOURS_ADD('2023-07-13 22:28:18', -3)")
    testFoldConst("SELECT HOURS_ADD(NULL, 1)")
    testFoldConst("SELECT HOURS_ADD('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT HOURS_ADD('2023-12-31 22:00:00+08:00', 3)")
    testFoldConst("SELECT HOURS_ADD('2023-01-01 00:00:00Z', 24)")
    testFoldConst("SELECT HOURS_ADD('2024-02-29 12:30:45.123-05:00', 12)")
    testFoldConst("SELECT HOURS_ADD('0001-01-01 00:00:00+00:00', 48)")
    testFoldConst("SELECT HOURS_ADD('2023-06-15 15:45:30.555+09:00', -6)")
    testFoldConst("SELECT HOURS_ADD('9999-12-31 23:00:00-12:00', 1)")
    testFoldConst("SELECT HOURS_ADD('2023-03-15 10:20:30+05:30', -18)")

    // 28. HOURS_DIFF function constant folding tests
    testFoldConst("SELECT HOURS_DIFF('2020-12-25 22:00:00', '2020-12-25 21:00:00')")
    testFoldConst("SELECT HOURS_DIFF('2023-07-14', '2023-07-13')")
    testFoldConst("SELECT HOURS_DIFF('2023-07-13 12:30:59', '2023-07-13 13:30:01')")
    testFoldConst("SELECT HOURS_DIFF(NULL, '2020-12-25 21:00:00')")
    testFoldConst("SELECT HOURS_DIFF('2020-12-25 22:00:00', NULL)")

    // 29. HOURS_SUB function constant folding tests
    testFoldConst("SELECT HOURS_SUB('2020-02-02 02:02:02', 1)")
    testFoldConst("SELECT HOURS_SUB('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT HOURS_SUB('2023-07-13 22:28:18', -3)")
    testFoldConst("SELECT HOURS_SUB(NULL, 1)")
    testFoldConst("SELECT HOURS_SUB('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT HOURS_SUB('2023-12-31 22:00:00+08:00', 3)")
    testFoldConst("SELECT HOURS_SUB('2023-01-01 00:00:00Z', 24)")
    testFoldConst("SELECT HOURS_SUB('2024-02-29 12:30:45.123-05:00', 12)")
    testFoldConst("SELECT HOURS_SUB('0001-01-01 00:00:00+00:00', 48)")
    testFoldConst("SELECT HOURS_SUB('2023-06-15 15:45:30.555+09:00', -6)")
    testFoldConst("SELECT HOURS_SUB('9999-12-31 23:00:00-12:00', 1)")
    testFoldConst("SELECT HOURS_SUB('2023-03-15 10:20:30+05:30', -18)")

    // Other functions in Group 2 (序号30-39)
    
    // 30. MAKEDATE function constant folding tests
    testFoldConst("SELECT MAKEDATE(2023, 100)")
    testFoldConst("SELECT MAKEDATE(2021, 365)")
    testFoldConst("SELECT MAKEDATE(NULL, 100)")
    testFoldConst("SELECT MAKEDATE(2023, NULL)")

    // 32. MICROSECONDS_ADD function constant folding tests
    testFoldConst("SELECT MICROSECONDS_ADD('2020-02-02 02:02:02', 1000000)")
    testFoldConst("SELECT MICROSECONDS_ADD('2023-10-01 12:30:45.123456', 500000)")
    testFoldConst("SELECT MICROSECONDS_ADD('2023-10-01 10:00:00', -1000000)")
    testFoldConst("SELECT MICROSECONDS_ADD('2023-12-31 23:59:59.999999+08:00', 1000000)")
    testFoldConst("SELECT MICROSECONDS_ADD('2023-01-01 00:00:00Z', 500000)")
    testFoldConst("SELECT MICROSECONDS_ADD('2024-02-29 12:30:45.123456-05:00', 2000000)")
    testFoldConst("SELECT MICROSECONDS_ADD('0001-01-01 00:00:00+00:00', 999999)")
    testFoldConst("SELECT MICROSECONDS_ADD('2023-06-15 15:45:30.555555+09:00', -500000)")
    testFoldConst("SELECT MICROSECONDS_ADD('9999-12-31 23:59:59.123456-12:00', 1000000)")
    testFoldConst("SELECT MICROSECONDS_ADD('2023-03-15 10:20:30+05:30', -1000000)")

    // 33. MICROSECONDS_DIFF function constant folding tests
    testFoldConst("SELECT MICROSECONDS_DIFF('2020-12-25 22:00:00.123456', '2020-12-25 22:00:00.000000')")
    testFoldConst("SELECT MICROSECONDS_DIFF('2023-06-15 10:30:00', '2023-06-15 10:29:59')")

    // 34. MICROSECONDS_SUB function constant folding tests
    testFoldConst("SELECT MICROSECONDS_SUB('2020-02-02 02:02:02.123456', 500000)")
    testFoldConst("SELECT MICROSECONDS_SUB('2023-10-01 12:30:45', 1000000)")
    testFoldConst("SELECT MICROSECONDS_SUB('2023-10-01 10:00:00', -500000)")
    testFoldConst("SELECT MICROSECONDS_SUB('2023-12-31 23:59:59.999999+08:00', 500000)")
    testFoldConst("SELECT MICROSECONDS_SUB('2023-01-01 00:00:00Z', 1000000)")
    testFoldConst("SELECT MICROSECONDS_SUB('2024-02-29 12:30:45.123456-05:00', 1000000)")
    testFoldConst("SELECT MICROSECONDS_SUB('0001-01-01 00:00:00+00:00', 999999)")
    testFoldConst("SELECT MICROSECONDS_SUB('2023-06-15 15:45:30.555555+09:00', -500000)")
    testFoldConst("SELECT MICROSECONDS_SUB('9999-12-31 23:59:59.123456-12:00', 2000000)")
    testFoldConst("SELECT MICROSECONDS_SUB('2023-03-15 10:20:30+05:30', -1000000)")

    // 35. MICROSECOND_TIMESTAMP function constant folding tests
    testFoldConst("SELECT MICROSECOND_TIMESTAMP('2025-01-23 12:34:56.123456')")
    testFoldConst("SELECT MICROSECOND_TIMESTAMP('1970-01-01')")

    // 36. MICROSECOND function constant folding tests
    testFoldConst("SELECT MICROSECOND('2019-01-01 00:00:00.123456')")
    testFoldConst("SELECT MICROSECOND(CAST('14:30:25.123456' AS TIME))")
    testFoldConst("SELECT MICROSECOND('2019-01-01 00:00:00')")
    testFoldConst("SELECT MICROSECOND('2019-01-01')")
    testFoldConst("SELECT MICROSECOND('2023-05-01 10:05:30.999999')")

    // 38. MILLISECONDS_ADD function constant folding tests
    testFoldConst("SELECT MILLISECONDS_ADD('2020-02-02 02:02:02', 1000)")
    testFoldConst("SELECT MILLISECONDS_ADD('2023-10-01 12:30:45.123', 500)")
    testFoldConst("SELECT MILLISECONDS_ADD('2023-10-01 10:00:00', -1000)")
    testFoldConst("SELECT MILLISECONDS_ADD('2023-12-31 23:59:59.999+08:00', 1000)")
    testFoldConst("SELECT MILLISECONDS_ADD('2023-01-01 00:00:00Z', 500)")
    testFoldConst("SELECT MILLISECONDS_ADD('2024-02-29 12:30:45.123-05:00', 2000)")
    testFoldConst("SELECT MILLISECONDS_ADD('0001-01-01 00:00:00+00:00', 999)")
    testFoldConst("SELECT MILLISECONDS_ADD('2023-06-15 15:45:30.555+09:00', -500)")
    testFoldConst("SELECT MILLISECONDS_ADD('9999-12-31 23:59:59.123-12:00', 1000)")
    testFoldConst("SELECT MILLISECONDS_ADD('2023-03-15 10:20:30+05:30', -1000)")

    // 39. MILLISECONDS_DIFF function constant folding tests
    testFoldConst("SELECT MILLISECONDS_DIFF('2020-12-25 22:00:00.123', '2020-12-25 22:00:00.000')")
    testFoldConst("SELECT MILLISECONDS_DIFF('2023-06-15 10:30:00', '2023-06-15 10:29:59')")

    // Test constant folding for Group 3 remaining functions

    // 40. MILLISECONDS_SUB function constant folding tests
    testFoldConst("SELECT MILLISECONDS_SUB('2020-02-02 02:02:02.123', 500)")
    testFoldConst("SELECT MILLISECONDS_SUB('2023-10-01 12:30:45', 1000)")
    testFoldConst("SELECT MILLISECONDS_SUB('2023-10-01 10:00:00', -500)")
    testFoldConst("SELECT MILLISECONDS_SUB('2023-12-31 23:59:59.999+08:00', 500)")
    testFoldConst("SELECT MILLISECONDS_SUB('2023-01-01 00:00:00Z', 1000)")
    testFoldConst("SELECT MILLISECONDS_SUB('2024-02-29 12:30:45.123-05:00', 1000)")
    testFoldConst("SELECT MILLISECONDS_SUB('0001-01-01 00:00:00+00:00', 999)")
    testFoldConst("SELECT MILLISECONDS_SUB('2023-06-15 15:45:30.555+09:00', -500)")
    testFoldConst("SELECT MILLISECONDS_SUB('9999-12-31 23:59:59.123-12:00', 2000)")
    testFoldConst("SELECT MILLISECONDS_SUB('2023-03-15 10:20:30+05:30', -1000)")

    // 41. MILLISECONDS_DIFF function constant folding tests (additional)
    testFoldConst("SELECT MILLISECONDS_DIFF('2020-03-01 00:00:00.500', '2020-02-29 23:59:59.000')")

    // 43. MILLISECOND_TIMESTAMP function constant folding tests
    testFoldConst("SELECT MILLISECOND_TIMESTAMP('2025-01-23 12:34:56.123')")
    testFoldConst("SELECT MILLISECOND_TIMESTAMP('2024-01-01 00:00:00.123456')")
    testFoldConst("SELECT MILLISECOND_TIMESTAMP('1970-01-01')")

    // Test constant folding for Group 4 functions (Minute and Month functions)
    
    // 44. MINUTE_CEIL function constant folding tests
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18')")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18.123', 5)")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18', 5, '2023-07-13 22:20:00')")
    testFoldConst("SELECT MINUTE_CEIL(CAST('2023-07-13' AS DATE), 30)")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18.789123', '2023-07-13 22:20:00+08:00')")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18.123456+05:00', 5, '2023-07-13 22:20:00')")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18.654321', 5, '2023-07-13 22:20:00+08:00')")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 23:59:59.999+00:00')")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 23:59:59.111+00:00', '2023-07-13 23:50:00+08:00')")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:30.678-01:00', 15)")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 23:59:59.999+00:00', 10, '2023-07-13 23:50:00+08:00')")

    // 45. MINUTE_FLOOR function constant folding tests
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18')")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.123', 5)")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18', 5, '2023-07-13 22:20:00')")
    testFoldConst("SELECT MINUTE_FLOOR(CAST('2023-07-13' AS DATE), 30)")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.789123', '2023-07-13 22:20:00+08:00')")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.123456+05:00', 5, '2023-07-13 22:20:00')")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18.654321', 5, '2023-07-13 22:20:00+08:00')")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 23:59:59.999+00:00')")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 23:59:59.111+00:00', '2023-07-13 23:50:00+08:00')")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:30.678-01:00', 15)")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 23:59:59.999+00:00', 10, '2023-07-13 23:50:00+08:00')")

    // 46. MINUTE function constant folding tests
    testFoldConst("SELECT MINUTE('2018-12-31 23:59:59')")
    testFoldConst("SELECT MINUTE('2023-05-01 10:05:30.123456')")
    testFoldConst("SELECT MINUTE('14:25:45')")
    testFoldConst("SELECT MINUTE('2023-07-13')")

    // 47. MINUTES_ADD function constant folding tests
    testFoldConst("SELECT MINUTES_ADD('2020-02-02', 1)")
    testFoldConst("SELECT MINUTES_ADD('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT MINUTES_ADD('2023-07-13 22:28:18', -5)")
    testFoldConst("SELECT MINUTES_ADD('2023-12-31 23:59:59+08:00', 30)")
    testFoldConst("SELECT MINUTES_ADD('2023-01-01 00:00:00Z', 60)")
    testFoldConst("SELECT MINUTES_ADD('2024-02-29 12:34:56.123-05:00', 45)")
    testFoldConst("SELECT MINUTES_ADD('0001-01-01 00:00:00+00:00', 1)")
    testFoldConst("SELECT MINUTES_ADD('2023-06-15 15:45:30.555+09:00', -30)")
    testFoldConst("SELECT MINUTES_ADD('9999-12-31 23:59:59-12:00', 15)")
    testFoldConst("SELECT MINUTES_ADD('2023-03-15 10:20:30+05:30', -60)")

    // 48. MINUTES_DIFF function constant folding tests
    testFoldConst("SELECT MINUTES_DIFF('2020-12-25 22:00:00', '2020-12-25 21:00:00')")
    testFoldConst("SELECT MINUTES_DIFF('2023-07-13 21:50:00', '2023-07-13 22:00:00')")
    testFoldConst("SELECT MINUTES_DIFF('2023-07-14', '2023-07-13')")

    // 49. MINUTES_SUB function constant folding tests
    testFoldConst("SELECT MINUTES_SUB('2020-02-02 02:02:02', 1)")
    testFoldConst("SELECT MINUTES_SUB('2023-07-13 22:38:18.456789', 10)")
    testFoldConst("SELECT MINUTES_SUB('2023-07-13 22:23:18', -5)")
    testFoldConst("SELECT MINUTES_SUB('2023-12-31 23:59:59+08:00', 30)")
    testFoldConst("SELECT MINUTES_SUB('2023-01-01 00:00:00Z', 60)")
    testFoldConst("SELECT MINUTES_SUB('2024-02-29 12:34:56.123-05:00', 45)")
    testFoldConst("SELECT MINUTES_SUB('0001-01-01 00:00:00+00:00', 1)")
    testFoldConst("SELECT MINUTES_SUB('2023-06-15 15:45:30.555+09:00', -30)")
    testFoldConst("SELECT MINUTES_SUB('9999-12-31 23:59:59-12:00', 15)")
    testFoldConst("SELECT MINUTES_SUB('2023-03-15 10:20:30+05:30', -60)")

    // Month functions (序号50-57)
    
    // 50. MONTH_CEIL function constant folding tests
    testFoldConst("SELECT MONTH_CEIL('2023-07-13 22:28:18')")
    testFoldConst("SELECT MONTH_CEIL('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT MONTH_CEIL('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00')")
    testFoldConst("SELECT MONTH_CEIL(CAST('2023-07-13' AS DATE), 3)")

    // 51. MONTH_FLOOR function constant folding tests
    testFoldConst("SELECT MONTH_FLOOR('2023-07-13 22:28:18')")
    testFoldConst("SELECT MONTH_FLOOR('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT MONTH_FLOOR('2023-07-13 22:28:18', 5, '2023-01-01 00:00:00')")
    testFoldConst("SELECT MONTH_FLOOR(CAST('2023-07-13' AS DATE), 3)")

    // 52. MONTH function constant folding tests
    testFoldConst("SELECT MONTH('1987-01-01')")
    testFoldConst("SELECT MONTH('2023-07-13 22:28:18')")
    testFoldConst("SELECT MONTH('2023-12-05 10:15:30.456789')")

    // 53. MONTHNAME function constant folding tests
    testFoldConst("SELECT MONTHNAME('2008-02-03')")
    testFoldConst("SELECT MONTHNAME('2023-07-13 22:28:18')")

    // 54. MONTHS_ADD function constant folding tests
    testFoldConst("SELECT MONTHS_ADD('2020-01-31', 1)")
    testFoldConst("SELECT MONTHS_ADD('2020-01-31 02:02:02', 1)")
    testFoldConst("SELECT MONTHS_ADD('2020-01-31', -1)")
    testFoldConst("SELECT MONTHS_ADD('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT MONTHS_ADD('2023-12-31 23:59:59+08:00', 1)")
    testFoldConst("SELECT MONTHS_ADD('2023-01-15 10:30:45.000000Z', 2)")
    testFoldConst("SELECT MONTHS_ADD('2024-02-29 15:45:22.999999-05:00', 1)")
    testFoldConst("SELECT MONTHS_ADD('0001-01-01 00:00:00+00:00', 12)")
    testFoldConst("SELECT MONTHS_ADD('2023-06-30 23:59:59.555555+09:00', -6)")
    testFoldConst("SELECT MONTHS_ADD('9999-12-31 12:00:00-12:00', 6)")
    testFoldConst("SELECT MONTHS_ADD('2023-03-31 10:20:30+05:30', 3)")

    // 55. MONTHS_BETWEEN function constant folding tests
    testFoldConst("SELECT MONTHS_BETWEEN('2020-12-26', '2020-10-25')")
    testFoldConst("SELECT MONTHS_BETWEEN('2020-12-26 15:30:00', '2020-10-25 08:15:00')")
    testFoldConst("SELECT MONTHS_BETWEEN('2024-02-29', '2024-01-31')")
    testFoldConst("SELECT MONTHS_BETWEEN('2024-03-15', '2024-01-15')")

    // 56. MONTHS_DIFF function constant folding tests
    testFoldConst("SELECT MONTHS_DIFF('2020-03-28', '2020-02-29')")
    testFoldConst("SELECT MONTHS_DIFF('2020-03-29', '2020-02-29')")
    testFoldConst("SELECT MONTHS_DIFF('2020-03-30', '2020-02-29')")
    testFoldConst("SELECT MONTHS_DIFF('2023-07-15', '2023-07-30')")

    // 57. MONTHS_SUB function constant folding tests
    testFoldConst("SELECT MONTHS_SUB('2020-01-31', 1)")
    testFoldConst("SELECT MONTHS_SUB('2020-01-31 02:02:02', 1)")
    testFoldConst("SELECT MONTHS_SUB('2020-01-31', -1)")
    testFoldConst("SELECT MONTHS_SUB('2023-07-13 22:28:18', 5)")
    testFoldConst("SELECT MONTHS_SUB('2023-12-31 23:59:59+08:00', 1)")
    testFoldConst("SELECT MONTHS_SUB('2023-01-15 10:30:45.000000Z', 2)")
    testFoldConst("SELECT MONTHS_SUB('2024-02-29 15:45:22.999999-05:00', 1)")
    testFoldConst("SELECT MONTHS_SUB('0001-01-01 00:00:00+00:00', 12)")
    testFoldConst("SELECT MONTHS_SUB('2023-06-30 23:59:59.555555+09:00', -6)")
    testFoldConst("SELECT MONTHS_SUB('9999-12-31 12:00:00-12:00', 6)")
    testFoldConst("SELECT MONTHS_SUB('2023-03-31 10:20:30+05:30', 3)")

    // Other Group 4 functions (序号58, 60-62)
    
    // 58. NEXT_DAY function constant folding tests
    testFoldConst("SELECT NEXT_DAY('2020-01-31', 'MONDAY')")
    testFoldConst("SELECT NEXT_DAY('2020-01-31 02:02:02', 'MON')")
    testFoldConst("SELECT NEXT_DAY('2023-07-17', 'MON')")
    testFoldConst("SELECT NEXT_DAY('2023-07-13', 'FR')")

    // 60. QUARTER function constant folding tests
    testFoldConst("SELECT QUARTER('2025-01-16')")
    testFoldConst("SELECT QUARTER('2025-01-16 01:11:10')")
    testFoldConst("SELECT QUARTER('2023-05-20')")
    testFoldConst("SELECT QUARTER('2024-09-30 23:59:59')")
    testFoldConst("SELECT QUARTER('2022-12-01')")

    // 61. QUARTERS_ADD function constant folding tests
    testFoldConst("SELECT QUARTERS_ADD('2020-01-31', 1)")
    testFoldConst("SELECT QUARTERS_ADD('2020-01-31 02:02:02', 1)")
    testFoldConst("SELECT QUARTERS_ADD('2020-04-30', -1)")
    testFoldConst("SELECT QUARTERS_ADD('2023-07-13 22:28:18', 2)")
    testFoldConst("SELECT QUARTERS_ADD('2023-10-01', 2)")
    testFoldConst("SELECT QUARTERS_ADD('2023-12-31 23:59:59+08:00', 1)")
    testFoldConst("SELECT QUARTERS_ADD('2023-01-01 00:00:00Z', 2)")
    testFoldConst("SELECT QUARTERS_ADD('2024-02-29 12:30:45.123-05:00', 1)")
    testFoldConst("SELECT QUARTERS_ADD('0001-01-01 00:00:00+00:00', 4)")
    testFoldConst("SELECT QUARTERS_ADD('2023-06-15 15:45:30.555+09:00', -2)")
    testFoldConst("SELECT QUARTERS_ADD('9999-12-31 12:00:00-12:00', 1)")

    // 62. QUARTERS_SUB function constant folding tests
    testFoldConst("SELECT QUARTERS_SUB('2020-01-31', 1)")
    testFoldConst("SELECT QUARTERS_SUB('2020-01-31 02:02:02', 1)")
    testFoldConst("SELECT QUARTERS_SUB('2019-10-31', -1)")
    testFoldConst("SELECT QUARTERS_SUB('2023-07-13 22:28:18', 2)")
    testFoldConst("SELECT QUARTERS_SUB('2024-04-01', 2)")
    testFoldConst("SELECT QUARTERS_SUB('2023-12-31 23:59:59+08:00', 1)")
    testFoldConst("SELECT QUARTERS_SUB('2023-01-01 00:00:00Z', 2)")
    testFoldConst("SELECT QUARTERS_SUB('2024-02-29 12:30:45.123-05:00', 1)")
    testFoldConst("SELECT QUARTERS_SUB('0001-01-01 00:00:00+00:00', 4)")
    testFoldConst("SELECT QUARTERS_SUB('2023-06-15 15:45:30.555+09:00', -2)")
    testFoldConst("SELECT QUARTERS_SUB('9999-12-31 12:00:00-12:00', 1)")

    // Test constant folding for Group 5 functions (Second and time manipulation functions)
    
    // 63. SECOND_CEIL function constant folding tests
    testFoldConst("SELECT SECOND_CEIL('2025-01-23 12:34:56')")
    testFoldConst("SELECT SECOND_CEIL('2025-01-23 12:34:56', 5)")
    testFoldConst("SELECT SECOND_CEIL('2025-01-23 12:34:56', 10, '2025-01-23 12:00:00')")
    testFoldConst("SELECT SECOND_CEIL('2025-01-23 12:34:56.789', 5)")
    testFoldConst("SELECT SECOND_CEIL('2023-07-13 22:28:18.456789+05:00', 5)")
    testFoldConst("SELECT SECOND_CEIL('2023-07-13 22:28:18.456789+05:00', 10, '2023-07-13 22:28:00')")
    testFoldConst("SELECT SECOND_CEIL('2023-07-13 22:28:18.654321', 10, '2023-07-13 22:28:00+08:00')")
    testFoldConst("SELECT SECOND_CEIL('0001-01-01 00:00:00.000001+08:00', 5)")
    testFoldConst("SELECT SECOND_CEIL('2025-12-31 23:59:59.999+00:00', 20, '2025-12-31 23:59:00+08:00')")
    testFoldConst("SELECT SECOND_CEIL('2023-06-30 23:59:59.500+00:00', 30)")
    testFoldConst("SELECT SECOND_CEIL('2023-07-13 22:28:18+05:00', 30, '0001-01-01 00:00:00')")
    testFoldConst("SELECT SECOND_CEIL('9999-12-31 10:30:45+05:00', 30, '9999-12-31 10:30:00')")
    testFoldConst("SELECT SECOND_CEIL('2023-01-01 23:59:59.999+08:00', 30)")
    testFoldConst("SELECT SECOND_CEIL('2023-06-30 23:59:59.999+08:00', 30, '2023-06-30 23:59:00')")
    testFoldConst("SELECT SECOND_CEIL('2023-12-31 23:59:59.999+00:00', 30)")
    testFoldConst("SELECT SECOND_CEIL('0001-01-01 23:59:59.999+08:00', 20)")
    testFoldConst("SELECT SECOND_CEIL('2023-07-13 12:00:00.000001+08:00', 30)")
    testFoldConst("SELECT SECOND_CEIL('2023-07-13 23:59:59.999999+08:00', 30)")
    testFoldConst("SELECT SECOND_CEIL('9999-12-31 20:55:59+05:00');")

    // 64. SECOND_FLOOR function constant folding tests
    testFoldConst("SELECT SECOND_FLOOR('2025-01-23 12:34:56')")
    testFoldConst("SELECT SECOND_FLOOR('2025-01-23 12:34:56', 5)")
    testFoldConst("SELECT SECOND_FLOOR('2025-01-23 12:34:56', 10, '2025-01-23 12:00:00')")
    testFoldConst("SELECT SECOND_FLOOR('2025-01-23 12:34:56.789', 5)")
    testFoldConst("SELECT SECOND_FLOOR('2023-07-13 22:28:18.456789+05:00', 5)")
    testFoldConst("SELECT SECOND_FLOOR('2023-07-13 22:28:18.456789+05:00', 10, '2023-07-13 22:28:00')")
    testFoldConst("SELECT SECOND_FLOOR('2023-07-13 22:28:18.654321', 10, '2023-07-13 22:28:00+08:00')")
    testFoldConst("SELECT SECOND_FLOOR('0001-01-01 00:00:00.000001+08:00', 5)")
    testFoldConst("SELECT SECOND_FLOOR('9999-12-31 23:59:59.999999-02:00', 5)")
    testFoldConst("SELECT SECOND_FLOOR('2025-12-31 23:59:59.999+00:00', 20, '2025-12-31 23:59:00+08:00')")
    testFoldConst("SELECT SECOND_FLOOR('2023-06-30 23:59:59.500+00:00', 30)")
    testFoldConst("SELECT SECOND_FLOOR('2023-07-13 22:28:18+05:00', 30, '0001-01-01 00:00:00')")
    testFoldConst("SELECT SECOND_FLOOR('9999-12-31 10:30:45+05:00', 30, '9999-12-31 10:30:00')")
    testFoldConst("SELECT SECOND_FLOOR('2023-01-01 23:59:59.999+08:00', 30)")
    testFoldConst("SELECT SECOND_FLOOR('2023-06-30 23:59:59.999+08:00', 30, '2023-06-30 23:59:00')")
    testFoldConst("SELECT SECOND_FLOOR('2023-12-31 23:59:59.999+00:00', 30)")
    testFoldConst("SELECT SECOND_FLOOR('0001-01-01 23:59:59.999+08:00', 20)")
    testFoldConst("SELECT SECOND_FLOOR('9999-12-31 23:59:59.123+08:00', 20)")
    testFoldConst("SELECT SECOND_FLOOR('2023-07-13 12:00:00.000001+08:00', 30)")
    testFoldConst("SELECT SECOND_FLOOR('2023-07-13 23:59:59.999999+08:00', 30)")

    // 65. SECOND function constant folding tests
    testFoldConst("SELECT SECOND('2018-12-31 23:59:59')")
    testFoldConst("SELECT SECOND(CAST('15:42:33' AS TIME))")
    testFoldConst("SELECT SECOND('2023-07-13')")
    testFoldConst("SELECT SECOND('2023-07-13 10:30:25.123456')")
    testFoldConst("SELECT SECOND('2024-01-01 00:00:00')")

    // 66. SECONDS_ADD function constant folding tests
    testFoldConst("SELECT SECONDS_ADD('2025-01-23 12:34:56', 30)")
    testFoldConst("SELECT SECONDS_ADD('2025-01-23 12:34:56', -30)")
    testFoldConst("SELECT SECONDS_ADD('2023-07-13 23:59:50', 15)")
    testFoldConst("SELECT SECONDS_ADD('2023-01-01', 3600)")
    testFoldConst("SELECT SECONDS_ADD('2023-12-31 23:59:59+08:00', 30)")
    testFoldConst("SELECT SECONDS_ADD('2023-01-01 00:00:00Z', 3600)")
    testFoldConst("SELECT SECONDS_ADD('2024-02-29 12:34:56.123-05:00', 15)")
    testFoldConst("SELECT SECONDS_ADD('0001-01-01 00:00:00+00:00', 60)")
    testFoldConst("SELECT SECONDS_ADD('2023-06-15 15:45:30.555+09:00', -30)")
    testFoldConst("SELECT SECONDS_ADD('9999-12-31 23:59:59-12:00', 1)")
    testFoldConst("SELECT SECONDS_ADD('2023-03-15 10:20:30+05:30', -60)")

    // 67. SECONDS_DIFF function constant folding tests
    testFoldConst("SELECT SECONDS_DIFF('2025-01-23 12:35:56', '2025-01-23 12:34:56')")
    testFoldConst("SELECT SECONDS_DIFF('2023-01-01 00:00:00', '2023-01-01 00:01:00')")
    testFoldConst("SELECT SECONDS_DIFF('2023-01-02', '2023-01-01')")
    testFoldConst("SELECT SECONDS_DIFF('2023-07-13 12:00:00.123', '2023-07-13 11:59:59')")

    // 68. SECONDS_SUB function constant folding tests
    testFoldConst("SELECT SECONDS_SUB('2025-01-23 12:34:56', 30)")
    testFoldConst("SELECT SECONDS_SUB('2025-01-23 12:34:56', -30)")
    testFoldConst("SELECT SECONDS_SUB('2023-07-14 00:00:10', 15)")
    testFoldConst("SELECT SECONDS_SUB('2023-01-01', 3600)")
    testFoldConst("SELECT SECONDS_SUB('2023-12-31 23:59:59+08:00', 30)")
    testFoldConst("SELECT SECONDS_SUB('2023-01-01 00:00:00Z', 3600)")
    testFoldConst("SELECT SECONDS_SUB('2024-02-29 12:34:56.123-05:00', 15)")
    testFoldConst("SELECT SECONDS_SUB('0001-01-01 00:00:00+00:00', 60)")
    testFoldConst("SELECT SECONDS_SUB('2023-06-15 15:45:30.555+09:00', -30)")
    testFoldConst("SELECT SECONDS_SUB('9999-12-31 23:59:59-12:00', 1)")
    testFoldConst("SELECT SECONDS_SUB('2023-03-15 10:20:30+05:30', -60)")

    // 69. SECOND_TIMESTAMP function constant folding tests
    testFoldConst("SELECT SECOND_TIMESTAMP('1970-01-01 00:00:00 UTC')")
    testFoldConst("SELECT SECOND_TIMESTAMP('2025-01-23 12:34:56')")
    testFoldConst("SELECT SECOND_TIMESTAMP('2023-01-01')")
    testFoldConst("SELECT SECOND_TIMESTAMP('2023-07-13 22:28:18.456789')")

    // 70. SEC_TO_TIME function constant folding tests
    testFoldConst("SELECT SEC_TO_TIME(59738)")
    testFoldConst("SELECT SEC_TO_TIME(90061)")
    testFoldConst("SELECT SEC_TO_TIME(-3600)")
    testFoldConst("SELECT SEC_TO_TIME(0)")
    testFoldConst("SELECT SEC_TO_TIME(3661.9)")

    // 71. STR_TO_DATE function constant folding tests
    testFoldConst("SELECT STR_TO_DATE('2025-01-23 12:34:56', '%Y-%m-%d %H:%i:%s')")
    testFoldConst("SELECT STR_TO_DATE('2025-01-23 12:34:56', 'yyyy-MM-dd HH:mm:ss')")
    testFoldConst("SELECT STR_TO_DATE('20230713', 'yyyyMMdd')")
    testFoldConst("SELECT STR_TO_DATE('15:30:45', '%H:%i:%s')")
    testFoldConst("SELECT STR_TO_DATE('200442 Monday', '%X%V %W')")
    testFoldConst("SELECT STR_TO_DATE('Oct 5 2023 3:45:00 PM', '%b %d %Y %h:%i:%s %p')")
    testFoldConst("SELECT STR_TO_DATE('2023-01-01 10:00:00 (GMT)', '%Y-%m-%d %H:%i:%s')")
    testFoldConst("SELECT STR_TO_DATE('2023-07-13 12:34:56.789', '%Y-%m-%d %H:%i:%s.%f')")
    testFoldConst("SELECT STR_TO_DATE('2023-01-01', '')")

    // 72. TIMEDIFF function constant folding tests
    testFoldConst("SELECT TIMEDIFF('2024-07-20 16:59:30', '2024-07-11 16:35:21')")
    testFoldConst("SELECT TIMEDIFF('2023-10-05 15:45:00', '2023-10-05')")
    testFoldConst("SELECT TIMEDIFF('2023-01-01 09:00:00', '2023-01-01 10:30:00')")
    testFoldConst("SELECT TIMEDIFF('2023-12-31 23:59:59', '2023-12-31 23:59:50')")

    // 73. TIME function constant folding tests
    testFoldConst("SELECT TIME('2025-1-1 12:12:12')")

    // 74. TIMESTAMPADD function constant folding tests
    testFoldConst("SELECT TIMESTAMPADD(MINUTE, 1, '2019-01-02')")
    testFoldConst("SELECT TIMESTAMPADD(WEEK, 1, '2019-01-02')")
    testFoldConst("SELECT TIMESTAMPADD(HOUR, -3, '2023-07-13 10:30:00')")
    testFoldConst("SELECT TIMESTAMPADD(MONTH, 1, '2023-01-31')")
    testFoldConst("SELECT TIMESTAMPADD(YEAR, 1, '2023-12-31 23:59:59')")

    // 75. TIMESTAMPDIFF function constant folding tests
    testFoldConst("SELECT TIMESTAMPDIFF(MONTH, '2003-02-01', '2003-05-01')")
    testFoldConst("SELECT TIMESTAMPDIFF(YEAR, '2002-05-01', '2001-01-01')")
    testFoldConst("SELECT TIMESTAMPDIFF(MINUTE, '2003-02-01', '2003-05-01 12:05:55')")
    testFoldConst("SELECT TIMESTAMPDIFF(DAY, '2023-12-31 23:59:50', '2024-01-01 00:00:05')")
    testFoldConst("SELECT TIMESTAMPDIFF(MONTH, '2023-01-31', '2023-02-28')")
    testFoldConst("SELECT TIMESTAMPDIFF(MONTH, '2023-01-31', '2023-02-27')")
    testFoldConst("SELECT TIMESTAMPDIFF(WEEK, '2023-01-01', '2023-01-15')")

    // 76. TIMESTAMP function constant folding tests
    testFoldConst("SELECT TIMESTAMP('2019-01-01 12:00:00')")
    testFoldConst("SELECT TIMESTAMP('2019-01-01')")

    // 77. TIME_TO_SEC function constant folding tests
    testFoldConst("SELECT TIME_TO_SEC('16:32:18')")
    testFoldConst("SELECT TIME_TO_SEC('2025-01-01 16:32:18')")
    testFoldConst("SELECT TIME_TO_SEC('-02:30:00')")
    testFoldConst("SELECT TIME_TO_SEC('-16:32:18.99')")
    testFoldConst("SELECT TIME_TO_SEC('10:15:30.123456')")
    testFoldConst("SELECT TIME_TO_SEC('12:60:00')")
    testFoldConst("SELECT TIME_TO_SEC('839:00:00')")

    // Test constant folding for Group 6 functions (TO_DATE, TO_DAYS, TO_ISO8601, TO_MONDAY, UNIX_TIMESTAMP, UTC_TIMESTAMP, WEEK functions)
    
    // 78. TO_DATE function constant folding tests
    testFoldConst("SELECT TO_DATE('2020-02-02 00:00:00')")
    testFoldConst("SELECT TO_DATE('2020-02-02')")

    // 79. TO_DAYS function constant folding tests
    testFoldConst("SELECT TO_DAYS('2007-10-07')")
    testFoldConst("SELECT TO_DAYS('2007-10-07 10:03:09')")
    testFoldConst("SELECT TO_DAYS('0000-01-01 00:00:00')")
    testFoldConst("SELECT TO_DAYS('0000-02-28')")
    testFoldConst("SELECT TO_DAYS('0000-02-29')")
    testFoldConst("SELECT TO_DAYS('0000-03-01')")

    // 80. TO_ISO8601 function constant folding tests
    testFoldConst("SELECT TO_ISO8601(CAST('2023-10-05' AS DATE)) AS date_result")
    testFoldConst("SELECT TO_ISO8601(CAST('2020-01-01 12:30:45' AS DATETIME)) AS datetime_result")
    testFoldConst("SELECT TO_ISO8601(CAST('2020-01-01 12:30:45.956' AS DATETIME)) AS datetime_result")

    // 81. TO_MONDAY function constant folding tests
    testFoldConst("SELECT TO_MONDAY('2022-09-10') AS result")
    testFoldConst("SELECT TO_MONDAY('1022-09-10') AS result")
    testFoldConst("SELECT TO_MONDAY('2023-10-09') AS result")
    testFoldConst("SELECT TO_MONDAY('1970-01-02'), TO_MONDAY('1970-01-01'), TO_MONDAY('1970-01-03'), TO_MONDAY('1970-01-04')")

    // 82. UNIX_TIMESTAMP function constant folding tests
    testFoldConst("SELECT UNIX_TIMESTAMP('1970-01-01 +08:00')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19 +09:00')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2007-11-30 10:30-19', '%Y-%m-%d %H:%i-%s')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2007-11-30 10:30%3A19', '%Y-%m-%d')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.123')")
    testFoldConst("SELECT UNIX_TIMESTAMP('1007-11-30 10:30:19')")
    testFoldConst("SELECT UNIX_TIMESTAMP('2038-01-19 11:14:08', NULL)")

    // 84. WEEK_CEIL function constant folding tests
    testFoldConst("SELECT WEEK_CEIL(CAST('2023-07-13 22:28:18' AS DATETIME)) AS result")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18', 2) AS result")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13', 1, '2023-07-03') AS result")
    testFoldConst("SELECT WEEK_CEIL(CAST('2023-07-13' AS DATE))")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00')")
    testFoldConst("SELECT WEEK_CEIL('2023-07-31 20:00:00+00:00', '2023-07-03 00:00:00+08:00')")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00', 2, '0001-01-01 00:00:00')")
    testFoldConst("SELECT WEEK_CEIL('9999-06-15 10:30:45+05:00', 2, '9999-06-07 00:00:00')")
    testFoldConst("SELECT WEEK_CEIL('2023-01-07 23:59:59.999+08:00', 2)")
    testFoldConst("SELECT WEEK_CEIL('2023-06-30 23:59:59.999+08:00', 2, '2023-01-02 00:00:00')")
    testFoldConst("SELECT WEEK_CEIL('2023-12-31 23:59:59.999+00:00', 2)")
    testFoldConst("SELECT WEEK_CEIL('0001-01-07 23:59:59.999+08:00', 1)")
    testFoldConst("SELECT WEEK_CEIL('2023-01-01 00:00:00.000001+08:00', 2)")
    testFoldConst("SELECT WEEK_CEIL('2023-07-08 23:59:59.999999+08:00', 2)")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18', '2023-07-03 00:00:00+08:00')")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00', 2)")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18+05:00', 2, '2023-07-03')")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18', 2, '2023-07-03 00:00:00+08:00')")
    testFoldConst("SELECT WEEK_CEIL('0001-01-08 12:00:00+08:00')")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 23:59:59+00:00', 2, '2023-07-03 00:00:00+08:00')")

    // 85. WEEKDAY function constant folding tests
    testFoldConst("SELECT WEEKDAY('2023-10-09')")
    testFoldConst("SELECT WEEKDAY('2023-10-15 18:30:00')")

    // 86. WEEK_FLOOR function constant folding tests
    testFoldConst("SELECT WEEK_FLOOR(CAST('2023-07-13 22:28:18' AS DATETIME)) AS result")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18', 2) AS result")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13', 1, '2023-07-03') AS result")
    testFoldConst("SELECT WEEK_FLOOR(CAST('2023-07-13' AS DATE)) AS result")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00')")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-31 20:00:00+00:00', '2023-07-03 00:00:00+08:00')")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00', 2, '0001-01-01 00:00:00')")
    testFoldConst("SELECT WEEK_FLOOR('9999-06-15 10:30:45+05:00', 2, '9999-06-07 00:00:00')")
    testFoldConst("SELECT WEEK_FLOOR('2023-01-07 23:59:59.999+08:00', 2)")
    testFoldConst("SELECT WEEK_FLOOR('2023-06-30 23:59:59.999+08:00', 2, '2023-01-02 00:00:00')")
    testFoldConst("SELECT WEEK_FLOOR('2023-12-31 23:59:59.999+00:00', 2)")
    testFoldConst("SELECT WEEK_FLOOR('0001-01-07 23:59:59.999+08:00', 1)")
    testFoldConst("SELECT WEEK_FLOOR('9999-12-31 12:00:00.123+08:00', 1)")
    testFoldConst("SELECT WEEK_FLOOR('2023-01-01 00:00:00.000001+08:00', 2)")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-08 23:59:59.999999+08:00', 2)")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18', '2023-07-03 00:00:00+08:00')")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00', 2)")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18+05:00', 2, '2023-07-03')")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18', 2, '2023-07-03 00:00:00+08:00')")
    testFoldConst("SELECT WEEK_FLOOR('0001-01-08 12:00:00+08:00')")
    testFoldConst("SELECT WEEK_FLOOR('9999-12-27 12:00:00+08:00')")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 23:59:59+00:00', 2, '2023-07-03 00:00:00+08:00')")

    // 87. WEEK function constant folding tests
    testFoldConst("SELECT WEEK('2020-01-01') AS week_result")
    testFoldConst("SELECT WEEK('2020-07-01', 1) AS week_result")
    testFoldConst("SELECT WEEK('2023-01-01', 0) AS mode_0, WEEK('2023-01-01', 3) AS mode_3")
    testFoldConst("SELECT WEEK('2023-01-01', 7) AS week_result")
    testFoldConst("SELECT WEEK('2023-01-01', -1) AS week_result")
    testFoldConst("SELECT WEEK('2023-12-31 23:59:59', 3) AS week_result")

    // 88. WEEKOFYEAR function constant folding tests
    testFoldConst("SELECT WEEKOFYEAR('2023-05-01') AS week_20230501")
    testFoldConst("SELECT WEEKOFYEAR('2023-01-02') AS week_20230102")
    testFoldConst("SELECT WEEKOFYEAR('2023-12-25') AS week_20231225")
    testFoldConst("SELECT WEEKOFYEAR('1023-01-04')")
    testFoldConst("SELECT WEEKOFYEAR('2024-01-01') AS week_20240101")

    // 89. WEEKS_ADD function constant folding tests
    testFoldConst("SELECT WEEKS_ADD('2023-10-01 08:30:45', 1) AS add_1_week_datetime")
    testFoldConst("SELECT WEEKS_ADD('2023-10-01 14:20:10', -1) AS subtract_1_week_datetime")
    testFoldConst("SELECT WEEKS_ADD('2023-05-20', 2) AS add_2_week_date")
    testFoldConst("SELECT WEEKS_ADD('2023-12-25', 1) AS cross_year_add")
    testFoldConst("SELECT WEEKS_ADD('2023-12-31 23:59:59+08:00', 1) AS add_1_week_tz")
    testFoldConst("SELECT WEEKS_ADD('2023-01-01 00:00:00Z', 2) AS add_2_week_utc")
    testFoldConst("SELECT WEEKS_ADD('2024-02-29 12:30:45.123-05:00', 1) AS leap_year_tz")
    testFoldConst("SELECT WEEKS_ADD('0001-01-01 00:00:00+00:00', 4) AS ancient_tz")
    testFoldConst("SELECT WEEKS_ADD('2023-06-15 15:45:30.555+09:00', -2) AS subtract_2_week_tz")
    testFoldConst("SELECT WEEKS_ADD('9999-12-31 12:00:00-12:00', 1) AS future_tz")

    // 90. WEEKS_DIFF function constant folding tests
    testFoldConst("SELECT WEEKS_DIFF('2020-12-25', '2020-10-25') AS diff_date")
    testFoldConst("SELECT WEEKS_DIFF('2020-12-25 10:10:02', '2020-10-25 12:10:02') AS diff_datetime")
    testFoldConst("SELECT WEEKS_DIFF('2023-10-07', '2023-10-01') AS diff_6_days")
    testFoldConst("SELECT WEEKS_DIFF('2023-10-09', '2023-10-01') AS diff_8_days")
    testFoldConst("SELECT WEEKS_DIFF('2024-01-01', '2023-12-25') AS cross_year")

    // 91. WEEKS_SUB function constant folding tests
    testFoldConst("SELECT WEEKS_SUB('2023-10-01 08:30:45', 1) AS sub_1_week_datetime")
    testFoldConst("SELECT WEEKS_SUB('2023-09-24 14:20:10', -1) AS add_1_week_datetime")
    testFoldConst("SELECT WEEKS_SUB('2023-06-03', 2) AS sub_2_week_date")
    testFoldConst("SELECT WEEKS_SUB('2024-01-01', 1) AS cross_year_sub")
    testFoldConst("SELECT WEEKS_SUB('2023-12-31 23:59:59+08:00', 1) AS sub_1_week_tz")
    testFoldConst("SELECT WEEKS_SUB('2023-01-01 00:00:00Z', 2) AS sub_2_week_utc")
    testFoldConst("SELECT WEEKS_SUB('2024-02-29 12:30:45.123-05:00', 1) AS leap_year_tz")
    testFoldConst("SELECT WEEKS_SUB('0001-01-01 00:00:00+00:00', 4) AS ancient_tz")
    testFoldConst("SELECT WEEKS_SUB('2023-06-15 15:45:30.555+09:00', -2) AS add_2_week_tz")
    testFoldConst("SELECT WEEKS_SUB('9999-12-31 12:00:00-12:00', 1) AS future_tz")

    // Test constant folding for Group 7 functions (Year functions)
    
    // 92. YEAR_CEIL function constant folding tests
    testFoldConst("SELECT YEAR_CEIL('2023-07-13 22:28:18') AS result")
    testFoldConst("SELECT YEAR_CEIL('2023-07-13 22:28:18', 5) AS result")
    testFoldConst("SELECT YEAR_CEIL(CAST('2023-07-13' AS DATE)) AS result")
    testFoldConst("SELECT YEAR_CEIL('2023-07-13', 1, '2020-01-01') AS result")
    testFoldConst("SELECT YEAR_CEIL('2023-01-01', 1, '2023-01-01') AS result")
    testFoldConst("SELECT YEAR_CEIL('2023-07-13 22:28:18', 2, '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT YEAR_CEIL('0001-06-15 12:30:00+08:00')")
    testFoldConst("SELECT YEAR_CEIL('9998-06-15 12:30:00+08:00')")
    testFoldConst("SELECT YEAR_CEIL('2025-12-31 20:00:00+00:00', 3, '2020-01-01 00:00:00+08:00')")

    // 93. YEAR_FLOOR function constant folding tests
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18') AS result")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18', 5) AS result")
    testFoldConst("SELECT YEAR_FLOOR(CAST('2023-07-13' AS DATE)) AS result")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13', 1, '2020-01-01') AS result")
    testFoldConst("SELECT YEAR_FLOOR('2025-07-13', 3, '2020-01-01') AS result")
    testFoldConst("SELECT YEAR_FLOOR('2025-12-31 20:00:00+00:00', '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18', '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18+05:00', 5)")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18+05:00', 2, '2020-01-01')")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18', 2, '2020-01-01 00:00:00+08:00')")
    testFoldConst("SELECT YEAR_FLOOR('0001-06-15 12:30:00+08:00')")
    testFoldConst("SELECT YEAR_FLOOR('9999-06-15 12:30:00+08:00')")
    testFoldConst("SELECT YEAR_FLOOR('2025-12-31 20:00:00+00:00', 3, '2020-01-01 00:00:00+08:00')")

    // 94. YEAR function constant folding tests
    testFoldConst("SELECT YEAR('1987-01-01') AS year_date")
    testFoldConst("SELECT YEAR('2024-05-20 14:30:25') AS year_datetime")
    testFoldConst("SELECT YEAR('2023-02-30') AS invalid_date")

    // 95. YEAR_OF_WEEK function constant folding tests
    testFoldConst("SELECT YEAR_OF_WEEK('2005-01-01') AS yow_result")
    testFoldConst("SELECT YOW('2005-01-01') AS yow_alias_result")
    testFoldConst("SELECT YEAR_OF_WEEK('2005-01-03') AS yow_result")
    testFoldConst("SELECT YEAR_OF_WEEK('2023-01-01') AS yow_result")
    testFoldConst("SELECT YEAR_OF_WEEK('2005-01-01 15:30:45') AS yow_datetime")
    testFoldConst("SELECT YEAR_OF_WEEK('2024-12-30') AS yow_result")

    // 96. YEARS_ADD function constant folding tests
    testFoldConst("SELECT YEARS_ADD('2020-01-31 02:02:02', 1) AS add_1_year_datetime")
    testFoldConst("SELECT YEARS_ADD('2023-05-10 15:40:20', -1) AS subtract_1_year_datetime")
    testFoldConst("SELECT YEARS_ADD('2019-12-25', 3) AS add_3_year_date")
    testFoldConst("SELECT YEARS_ADD('2020-02-29', 1) AS leap_day_adjust")
    testFoldConst("SELECT YEARS_ADD('2023-12-31 23:59:59+08:00', 1)")
    testFoldConst("SELECT YEARS_ADD('2023-01-01 00:00:00Z', 2)")
    testFoldConst("SELECT YEARS_ADD('2024-02-29 12:30:45.123-05:00', 1)")
    testFoldConst("SELECT YEARS_ADD('0001-01-01 00:00:00+00:00', 10)")
    testFoldConst("SELECT YEARS_ADD('2023-06-15 15:45:30.555+09:00', -3)")
    testFoldConst("SELECT YEARS_ADD('9999-12-31 12:00:00-12:00', -1)")

    // 97. YEARS_DIFF function constant folding tests
    testFoldConst("SELECT YEARS_DIFF('2020-12-25', '2019-12-25') AS diff_full_year")
    testFoldConst("SELECT YEARS_DIFF('2020-11-25', '2019-12-25') AS diff_less_than_year")
    testFoldConst("SELECT YEARS_DIFF('2022-03-15 08:30:00', '2021-03-15 09:10:00') AS diff_datetime")
    testFoldConst("SELECT YEARS_DIFF('2024-05-20', '2020-05-20 12:00:00') AS diff_mixed")
    testFoldConst("SELECT YEARS_DIFF('2024-02-29', '2023-02-28') AS leap_year_diff")

    // 98. YEARS_SUB function constant folding tests
    testFoldConst("SELECT YEARS_SUB('2020-02-02 02:02:02', 1) AS sub_1_year_datetime")
    testFoldConst("SELECT YEARS_SUB('2022-05-10 15:40:20', -1) AS add_1_year_datetime")
    testFoldConst("SELECT YEARS_SUB('2022-12-25', 3) AS sub_3_year_date")
    testFoldConst("SELECT YEARS_SUB('2020-02-29', 1) AS leap_day_adjust_1")
    testFoldConst("SELECT YEARS_SUB('2023-12-31 23:59:59+08:00', 1)")
    testFoldConst("SELECT YEARS_SUB('2023-01-01 00:00:00Z', 2)")
    testFoldConst("SELECT YEARS_SUB('2024-02-29 12:30:45.123-05:00', 1)")
    testFoldConst("SELECT YEARS_SUB('9999-01-01 00:00:00+00:00', 10)")
    testFoldConst("SELECT YEARS_SUB('2023-06-15 15:45:30.555+09:00', -3)")
    testFoldConst("SELECT YEARS_SUB('0001-12-31 12:00:00-12:00', -1)")

    // 99. MAKETIME function constant folding tests
    testFoldConst("SELECT MAKETIME(12, 15, 30)")
    testFoldConst("SELECT MAKETIME(111, 0, 23.1234567)")
    testFoldConst("SELECT MAKETIME(1234, 11, 4)")
    testFoldConst("SELECT MAKETIME(-1234, 6, 52)")
    testFoldConst("SELECT MAKETIME(20, 60, 12)")
    testFoldConst("SELECT MAKETIME(14, 51, 66)")
    testFoldConst("SELECT MAKETIME(NULL, 15, 16)")
    testFoldConst("SELECT MAKETIME(7, NULL, 8)")
    testFoldConst("SELECT MAKETIME(1, 2, NULL)")
    testFoldConst("SELECT MAKETIME(123, -4, 40)")
    testFoldConst("SELECT MAKETIME(7, 8, -23)")

    // 100. TIME_FORMAT function constant folding tests
    testFoldConst("SELECT TIME_FORMAT('00:00:00', '%H') AS zero_24hour")
    testFoldConst("SELECT TIME_FORMAT('00:00:00', '%k') AS zero_24hour_no_pad")
    testFoldConst("SELECT TIME_FORMAT('00:00:00', '%h') AS zero_12hour")
    testFoldConst("SELECT TIME_FORMAT('00:00:00', '%I') AS zero_12hour_alt")
    testFoldConst("SELECT TIME_FORMAT('00:00:00', '%l') AS zero_12hour_no_pad")
    testFoldConst("SELECT TIME_FORMAT('838:59:59', '%k:%i:%S') AS max_k_format")
    testFoldConst("SELECT TIME_FORMAT('838:59:59', '%H.%i.%s.%f') AS max_with_micro_sep")
    testFoldConst("SELECT TIME_FORMAT('838:59:59', '%T') AS max_time_T")
    testFoldConst("SELECT TIME_FORMAT('838:59:59', '%r') AS max_time_r")
    testFoldConst("SELECT TIME_FORMAT('-838:59:59', '%k %i %S') AS min_k_format")
    testFoldConst("SELECT TIME_FORMAT('-838:59:59', '%H%i%S%f') AS min_compact")
    testFoldConst("SELECT TIME_FORMAT('839:00:00', '%T') AS beyond_max_T")
    testFoldConst("SELECT TIME_FORMAT('-839:00:00', '%r') AS beyond_min_r")
    testFoldConst("SELECT TIME_FORMAT('12:34:56.123456', '%f') AS only_microseconds")
    testFoldConst("SELECT TIME_FORMAT('12:34:56.789012', '%k.%f') AS hour_microsec")
    testFoldConst("SELECT TIME_FORMAT('23:59:59.999999', '%T.%f') AS T_format_micro")
    testFoldConst("SELECT TIME_FORMAT('00:00:00.000001', '%f only') AS micro_with_text")
    testFoldConst("SELECT TIME_FORMAT('13:45:30', '%H vs %k vs %h vs %I vs %l') AS all_hour_formats")
    testFoldConst("SELECT TIME_FORMAT('03:07:09', '%H-%k-%h-%I-%l') AS morning_all_formats")
    testFoldConst("SELECT TIME_FORMAT('00:30:45', '%H|%k|%h|%I|%l') AS midnight_all_formats")
    testFoldConst("SELECT TIME_FORMAT('12:00:00', '%H/%k/%h/%I/%l') AS noon_all_formats")
    testFoldConst("SELECT TIME_FORMAT('23:59:59', '%k,%h,%l,%p') AS late_night_formats")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '%S') AS uppercase_S")
    testFoldConst("SELECT TIME_FORMAT('12:34:09', '%S vs %s') AS both_seconds")
    testFoldConst("SELECT TIME_FORMAT('12:34:05', '%k:%i:%S') AS k_i_S")
    testFoldConst("SELECT TIME_FORMAT('15:30:45', '%T') AS T_afternoon")
    testFoldConst("SELECT TIME_FORMAT('03:07:22', '%T') AS T_morning")
    testFoldConst("SELECT TIME_FORMAT('15:30:45', '%r') AS r_afternoon")
    testFoldConst("SELECT TIME_FORMAT('03:07:22', '%r') AS r_morning")
    testFoldConst("SELECT TIME_FORMAT('00:00:00', '%T vs %r') AS T_vs_r_midnight")
    testFoldConst("SELECT TIME_FORMAT('12:00:00', '%T vs %r') AS T_vs_r_noon")
    testFoldConst("SELECT TIME_FORMAT('13:45:30', '%p') AS only_pm")
    testFoldConst("SELECT TIME_FORMAT('09:15:20', '%p') AS only_am")
    testFoldConst("SELECT TIME_FORMAT('23:59:59', '%p at %l:%i') AS pm_natural")
    testFoldConst("SELECT TIME_FORMAT('00:30:45', '%p-%l-%i-%S') AS am_dashes")
    testFoldConst("SELECT TIME_FORMAT('15:07:22', '%p%p%p') AS triple_pm")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '%i') AS only_minutes")
    testFoldConst("SELECT TIME_FORMAT('12:05:56', '%i') AS minutes_leading_zero")
    testFoldConst("SELECT TIME_FORMAT('12:00:00', '%i:%S') AS min_sec_only")
    testFoldConst("SELECT TIME_FORMAT('23:59:59', '%i%S') AS min_sec_compact")
    testFoldConst("SELECT TIME_FORMAT('500:30:45', '%k:%i:%S') AS large_k")
    testFoldConst("SELECT TIME_FORMAT('700:00:00', '%H-%k') AS large_H_k")
    testFoldConst("SELECT TIME_FORMAT('100:15:30', '%T') AS large_T")
    testFoldConst("SELECT TIME_FORMAT('838:00:00', '%k only') AS max_hour_k")
    testFoldConst("SELECT TIME_FORMAT('-12:34:56', '%k:%i:%S') AS negative_k")
    testFoldConst("SELECT TIME_FORMAT('-100:30:45', '%T') AS negative_T")
    testFoldConst("SELECT TIME_FORMAT('-05:07:09', '%r') AS negative_r")
    testFoldConst("SELECT TIME_FORMAT('-838:59:59', '%H%k%h%I%l') AS negative_max_all")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '%%H=%%k') AS percent_escaped")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '%% %T %%') AS percent_around_T")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '%H\\:%i\\:%s') AS backslash_colon")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '%k-%i-%S-%f') AS all_with_dashes")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', '') AS empty_format")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', 'no specifiers at all') AS literal_only")
    testFoldConst("SELECT TIME_FORMAT('15:45:30', '%k%i%S%f%p%T%r') AS everything_combined")
    testFoldConst("SELECT TIME_FORMAT('03:07:09', '%l o clock %i minutes %S seconds %p') AS natural_lang")
    testFoldConst("SELECT TIME_FORMAT('23:59:59', '%H=%k, %h=%I=%l, %p') AS hour_comparisons")
    testFoldConst("SELECT TIME_FORMAT('12:00:00', 'Noon: %T or %r?') AS noon_question")
    testFoldConst("SELECT TIME_FORMAT('00:00:00', 'Midnight: %k|%h|%l %p') AS midnight_formats")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%k:%i:%S') AS datetime_k")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 03:07:22', '%l:%i %p') AS datetime_12h")
    testFoldConst("SELECT TIME_FORMAT(NULL, '%T') AS null_time_T")
    testFoldConst("SELECT TIME_FORMAT('12:34:56', NULL) AS null_format")
    testFoldConst("SELECT TIME_FORMAT(NULL, NULL) AS both_null")

    // TIME_FORMAT with date placeholders constant folding tests
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%Y-%m-%d %H:%i:%s') AS date_with_time")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 08:15:30.123456', '%y/%m/%d %T.%f') AS short_date_format")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 23:59:59', '%Y %m %d') AS year_month_day")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 12:34:56', '%c-%e') AS month_day_no_pad")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 10:20:30.987654', '%Y/%m/%d %H:%i:%s.%f') AS full_datetime")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%M') AS month_name")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%W') AS weekday_name")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%j') AS day_of_year")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%D') AS day_with_suffix")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%U %u') AS week_numbers")
    testFoldConst("SELECT TIME_FORMAT('2023-12-25 15:30:45', '%V %v %w') AS week_variants")

    // Additional NULL parameter tests for comprehensive coverage
    
    // MINUTE functions NULL tests
    testFoldConst("SELECT MINUTE_CEIL(NULL, 5)")
    testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT MINUTE_FLOOR(NULL, 5)")
    testFoldConst("SELECT MINUTE_FLOOR('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT MINUTE(NULL)")
    testFoldConst("SELECT MINUTES_ADD(NULL, 5)")
    testFoldConst("SELECT MINUTES_ADD('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT MINUTES_DIFF(NULL, '2023-07-13')")
    testFoldConst("SELECT MINUTES_DIFF('2023-07-13', NULL)")
    testFoldConst("SELECT MINUTES_SUB(NULL, 5)")
    testFoldConst("SELECT MINUTES_SUB('2023-07-13 22:28:18', NULL)")

    // MONTH functions NULL tests
    testFoldConst("SELECT MONTH_CEIL(NULL, 5)")
    testFoldConst("SELECT MONTH_CEIL('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT MONTH_FLOOR(NULL, 5)")
    testFoldConst("SELECT MONTH_FLOOR('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT MONTH(NULL)")
    testFoldConst("SELECT MONTHNAME(NULL)")
    testFoldConst("SELECT MONTHS_ADD(NULL, 1)")
    testFoldConst("SELECT MONTHS_ADD('2020-01-31', NULL)")
    testFoldConst("SELECT MONTHS_BETWEEN(NULL, '2020-10-25')")
    testFoldConst("SELECT MONTHS_BETWEEN('2020-12-26', NULL)")
    testFoldConst("SELECT MONTHS_DIFF(NULL, '2020-02-29')")
    testFoldConst("SELECT MONTHS_DIFF('2020-03-28', NULL)")
    testFoldConst("SELECT MONTHS_SUB(NULL, 1)")
    testFoldConst("SELECT MONTHS_SUB('2020-01-31', NULL)")

    // SECOND functions NULL tests
    testFoldConst("SELECT SECOND_CEIL(NULL, 5)")
    testFoldConst("SELECT SECOND_CEIL('2025-01-23 12:34:56', NULL)")
    testFoldConst("SELECT SECOND_FLOOR(NULL, 5)")
    testFoldConst("SELECT SECOND_FLOOR('2025-01-23 12:34:56', NULL)")
    testFoldConst("SELECT SECOND(NULL)")
    testFoldConst("SELECT SECONDS_ADD(NULL, 30)")
    testFoldConst("SELECT SECONDS_ADD('2025-01-23 12:34:56', NULL)")
    testFoldConst("SELECT SECONDS_DIFF(NULL, '2025-01-23 12:34:56')")
    testFoldConst("SELECT SECONDS_DIFF('2025-01-23 12:35:56', NULL)")
    testFoldConst("SELECT SECONDS_SUB(NULL, 30)")
    testFoldConst("SELECT SECONDS_SUB('2025-01-23 12:34:56', NULL)")

    // Other functions NULL tests
    testFoldConst("SELECT TO_DATE(NULL)")
    testFoldConst("SELECT TO_DAYS(NULL)")
    testFoldConst("SELECT TO_ISO8601(NULL)")
    testFoldConst("SELECT TO_MONDAY(NULL)")
    testFoldConst("SELECT UNIX_TIMESTAMP(NULL)")
    testFoldConst("SELECT WEEK_CEIL(NULL, 2)")
    testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT WEEK_FLOOR(NULL, 2)")
    testFoldConst("SELECT WEEK_FLOOR('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT WEEKDAY(NULL)")
    testFoldConst("SELECT WEEKOFYEAR(NULL)")
    testFoldConst("SELECT WEEKS_ADD(NULL, 1)")
    testFoldConst("SELECT WEEKS_ADD('2023-10-01', NULL)")
    testFoldConst("SELECT WEEKS_DIFF(NULL, '2020-10-25')")
    testFoldConst("SELECT WEEKS_DIFF('2020-12-25', NULL)")
    testFoldConst("SELECT WEEKS_SUB(NULL, 1)")
    testFoldConst("SELECT WEEKS_SUB('2023-10-01', NULL)")

    // YEAR functions NULL tests
    testFoldConst("SELECT YEAR_CEIL(NULL, 5)")
    testFoldConst("SELECT YEAR_CEIL('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT YEAR_FLOOR(NULL, 5)")
    testFoldConst("SELECT YEAR_FLOOR('2023-07-13 22:28:18', NULL)")
    testFoldConst("SELECT YEAR(NULL)")
    testFoldConst("SELECT YEAR_OF_WEEK(NULL)")
    testFoldConst("SELECT YEARS_ADD(NULL, 1)")
    testFoldConst("SELECT YEARS_ADD('2020-01-31', NULL)")
    testFoldConst("SELECT YEARS_DIFF(NULL, '2019-12-25')")
    testFoldConst("SELECT YEARS_DIFF('2020-12-25', NULL)")
    testFoldConst("SELECT YEARS_SUB(NULL, 1)")
    testFoldConst("SELECT YEARS_SUB('2020-02-02', NULL)")

    // TIMESTAMP with two args
    testFoldConst("SELECT TIMESTAMP('2025-11-01', '65:43:21')")
    testFoldConst("SELECT TIMESTAMP('2025-11-01 12:13:14', '1:23:45')")
    testFoldConst("SELECT TIMESTAMP('2025-1-1', '23:59:59')")
    testFoldConst("SELECT TIMESTAMP('2025-1-1 23:59:59', '1:23:45')")
    testFoldConst("SELECT TIMESTAMP('2025-1-31', '23:59:59')")
    testFoldConst("SELECT TIMESTAMP('2025-1-31 23:59:59', '1:23:45')")
    testFoldConst("SELECT TIMESTAMP('2025-12-31', '23:59:59')")
    testFoldConst("SELECT TIMESTAMP('2025-12-31 23:59:59', '1:23:45')")
    testFoldConst("SELECT TIMESTAMP(NULL, '1:23:45')")
    testFoldConst("SELECT TIMESTAMP('2025-11-01', NULL)")
    testFoldConst("SELECT TIMESTAMP('12:13:14', '11:45:14');")
    testFoldConst("SELECT TIMESTAMP('2026-01-05 11:45:14+05:30', '02:15:30');")

    // // Invalid parameter tests for error conditions
    // testFoldConst("SELECT DAY_CEIL('2023-07-13', -5)")
    // testFoldConst("SELECT HOUR_CEIL('2023-07-13 22:28:18', -5)")
    // testFoldConst("SELECT MINUTE_CEIL('2023-07-13 22:28:18', -5)")
    // testFoldConst("SELECT MONTH_CEIL('2023-07-13 22:28:18', -5)")
    // testFoldConst("SELECT SECOND_CEIL('2025-01-23 12:34:56', -5)")
    // testFoldConst("SELECT WEEK_CEIL('2023-07-13 22:28:18', -2)")
    // testFoldConst("SELECT YEAR_CEIL('2023-07-13 22:28:18', -5)")


    sql "DROP TABLE IF EXISTS test_datetime_ceil"

    sql """
    CREATE TABLE test_datetime_ceil (
        id INT,
        dt DATETIME,
        f DECIMAL
    ) ENGINE=OLAP
    PROPERTIES("replication_num" = "1");
    """

   sql "INSERT INTO test_datetime_ceil VALUES (1, '2025-10-10 12:34:56', 123.12);"
   sql "INSERT INTO test_datetime_ceil VALUES (2, '2025-01-01 00:00:00', 2.22);"
   sql "INSERT INTO test_datetime_ceil VALUES (3, '2025-12-31 23:59:59', 3.34);"

   qt_dateceil """
   SELECT
       dt,
        year_ceil(dt) AS year_ceil,
        month_ceil(dt) AS month_ceil,
        day_ceil(dt) AS day_ceil,
        hour_ceil(dt) AS hour_ceil,
        minute_ceil(dt) AS minute_ceil,
        second_ceil(dt) AS second_ceil
    FROM test_datetime_ceil ORDER BY id;
    """

    qt_todays """
    SELECT
        id,
        dt,
        to_days(dt) AS to_days_result
    FROM test_datetime_ceil ORDER BY id;
"""

    qt_ceil """
    SELECT
        id,
        ceil(f) AS ceil_f from test_datetime_ceil ORDER BY id;
    """

    testFoldConst("""SELECT
       dt,
        year_ceil(dt) AS year_ceil,
        month_ceil(dt) AS month_ceil,
        day_ceil(dt) AS day_ceil,
        hour_ceil(dt) AS hour_ceil,
        minute_ceil(dt) AS minute_ceil,
        second_ceil(dt) AS second_ceil
    FROM test_datetime_ceil ORDER BY id;""")

    testFoldConst("""SELECT
        id,
        dt,
        to_days(dt) AS to_days_result
    FROM test_datetime_ceil ORDER BY id;""")

    testFoldConst("""SELECT
        id,
        ceil(f) AS ceil_f from test_datetime_ceil ORDER BY id;""")

    // QUARTER_CEIL function tests
    qt_quarter_ceil_1 """SELECT QUARTER_CEIL('2023-07-13 22:28:18')"""
    qt_quarter_ceil_2 """SELECT QUARTER_CEIL('2023-07-13 22:28:18', 2)"""
    qt_quarter_ceil_3 """SELECT QUARTER_CEIL('2023-10-01 00:00:00', 2)"""
    qt_quarter_ceil_4 """SELECT QUARTER_CEIL('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00')"""
    qt_quarter_ceil_5 """SELECT QUARTER_CEIL('2023-07-13 22:28:18.456789', 2)"""
    qt_quarter_ceil_6 """SELECT QUARTER_CEIL('2023-07-13', 1)"""
    qt_quarter_ceil_7 """SELECT QUARTER_CEIL(NULL, 2), QUARTER_CEIL('2023-07-13 22:28:18', NULL)"""
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00')", '+11:00')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-12-31 20:00:00+00:00', '2023-01-01 00:00:00+08:00')", '-09:00')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 22:28:18', '2023-01-01 00:00:00+08:00')", '+04:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00', 2)", '-05:00')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00', 2, '2023-01-01')", '+07:00')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00+08:00')", '-11:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('0001-01-15 12:00:00+08:00')", '+02:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 23:59:59+00:00', 2, '2023-01-01 00:00:00+08:00')", '-03:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00', 2, '0001-01-01 00:00:00')", '+08:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-03-31 23:59:59.999+08:00', 2)", '-06:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-09-30 23:59:59.999+08:00', 2, '2023-01-01 00:00:00')", '+13:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-12-31 23:59:59.999+00:00', 2)", '-10:00')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('0001-03-31 23:59:59.999+08:00', 1)", '+05:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-01-01 00:00:00.000001+08:00', 2)", '-01:00')
    validateTimestamptzCeilFloor("SELECT QUARTER_CEIL('2023-06-30 23:59:59.999999+08:00', 2)", '+12:30')

    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18')")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18', 2)")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00')")
    testFoldConst("SELECT QUARTER_CEIL(CAST('2023-07-13' AS DATE), 1)")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00')")
    testFoldConst("SELECT QUARTER_CEIL('2023-12-31 20:00:00+00:00', '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18', '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00', 2)")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18+05:00', 2, '2023-01-01')")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT QUARTER_CEIL('0001-01-15 12:00:00+08:00')")
    testFoldConst("SELECT QUARTER_CEIL('2023-07-13 23:59:59+00:00', 2, '2023-01-01 00:00:00+08:00')")

    // QUARTER_FLOOR function tests
    qt_quarter_floor_1 """SELECT QUARTER_FLOOR('2023-07-13 22:28:18')"""
    qt_quarter_floor_2 """SELECT QUARTER_FLOOR('2023-07-13 22:28:18', 2)"""
    qt_quarter_floor_3 """SELECT QUARTER_FLOOR('2023-07-10 22:28:18', 2)"""
    qt_quarter_floor_4 """SELECT QUARTER_FLOOR('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00')"""
    qt_quarter_floor_5 """SELECT QUARTER_FLOOR('2023-07-13 22:28:18.456789', 2)"""
    qt_quarter_floor_6 """SELECT QUARTER_FLOOR('2023-07-13', 1)"""
    qt_quarter_floor_7 """SELECT QUARTER_FLOOR(NULL, 2), QUARTER_FLOOR('2023-07-13 22:28:18', NULL)"""
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00')", '+11:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-12-31 20:00:00+00:00', '2023-01-01 00:00:00+08:00')", '-09:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 22:28:18', '2023-01-01 00:00:00+08:00')", '+04:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00', 2)", '-05:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00', 2, '2023-01-01')", '+07:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00+08:00')", '-11:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('0001-01-15 12:00:00+08:00')", '+02:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('9999-12-15 12:00:00+08:00')", '-03:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 23:59:59+00:00', 2, '2023-01-01 00:00:00+08:00')", '+08:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00', 2, '0001-01-01 00:00:00')", '-06:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('9999-06-15 10:30:45+05:00', 1, '9999-01-01 00:00:00')", '+13:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-03-31 23:59:59.999+08:00', 2)", '-10:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-09-30 23:59:59.999+08:00', 2, '2023-01-01 00:00:00')", '+05:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-12-31 23:59:59.999+00:00', 2)", '-01:30')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('0001-03-31 23:59:59.999+08:00', 1)", '+12:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('9999-12-31 12:00:00.123+08:00', 1)", '-07:45')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-01-01 00:00:00.000001+08:00', 2)", '+01:15')
    validateTimestamptzCeilFloor("SELECT QUARTER_FLOOR('2023-06-30 23:59:59.999999+08:00', 2)", '-00:15')

    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18')")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18', 2)")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00')")
    testFoldConst("SELECT QUARTER_FLOOR(CAST('2023-07-13' AS DATE), 1)")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00')")
    testFoldConst("SELECT QUARTER_FLOOR('2023-12-31 20:00:00+00:00', '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18', '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00', 2)")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18+05:00', 2, '2023-01-01')")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 22:28:18', 2, '2023-01-01 00:00:00+08:00')")
    testFoldConst("SELECT QUARTER_FLOOR('0001-01-15 12:00:00+08:00')")
    testFoldConst("SELECT QUARTER_FLOOR('9999-12-15 12:00:00+08:00')")
    testFoldConst("SELECT QUARTER_FLOOR('2023-07-13 23:59:59+00:00', 2, '2023-01-01 00:00:00+08:00')")

}
