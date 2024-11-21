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

suite("test_external_brown", "p2,external,hive,external_remote,external_remote_hive") {
    Boolean ignoreP2 = true;
    if (ignoreP2) {
        logger.info("disable p2 test");
        return;
    }

    def formats = ["_parquet", "_orc", ""]
    def CPUNetworkUtilization_order = """
            SELECT machine_name,
                   MIN(cpu) AS cpu_min,
                   MAX(cpu) AS cpu_max,
                   AVG(cpu) AS cpu_avg,
                   MIN(net_in) AS net_in_min,
                   MAX(net_in) AS net_in_max,
                   AVG(net_in) AS net_in_avg,
                   MIN(net_out) AS net_out_min,
                   MAX(net_out) AS net_out_max,
                   AVG(net_out) AS net_out_avg
            FROM (
              SELECT machine_name,
                     COALESCE(cpu_user, 0.0) AS cpu,
                     COALESCE(bytes_in, 0.0) AS net_in,
                     COALESCE(bytes_out, 0.0) AS net_out
              FROM logs1SUFFIX
              WHERE machine_name IN ('anansi','aragog','urd')
                AND log_time >= TIMESTAMP '2017-01-11 00:00:00'
            ) AS r
            GROUP BY machine_name
            ORDER BY machine_name;"""
    def averageMetrics = """
            SELECT dt,
                   hr,
                   AVG(load_fifteen) AS load_fifteen_avg,
                   AVG(load_five) AS load_five_avg,
                   AVG(load_one) AS load_one_avg,
                   AVG(mem_free) AS mem_free_avg,
                   AVG(swap_free) AS swap_free_avg
            FROM (
              SELECT CAST(log_time AS DATE) AS dt,
                     EXTRACT(HOUR FROM log_time) AS hr,
                     load_fifteen,
                     load_five,
                     load_one,
                     mem_free,
                     swap_free
              FROM logs1SUFFIX
              WHERE machine_name = 'babbage'
                AND load_fifteen IS NOT NULL
                AND load_five IS NOT NULL
                AND load_one IS NOT NULL
                AND mem_free IS NOT NULL
                AND swap_free IS NOT NULL
                AND log_time >= TIMESTAMP '2017-01-01 00:00:00'
            ) AS r
            GROUP BY dt,
                     hr
            ORDER BY dt,
                     hr;"""
    def blockedIOServer = """
            SELECT machine_name,
                   COUNT(*) AS spikes
            FROM logs1SUFFIX
            WHERE machine_group = 'Servers'
              AND cpu_wio > 0.99
              AND log_time >= TIMESTAMP '2016-12-01 00:00:00'
              AND log_time < TIMESTAMP '2017-01-01 00:00:00'
            GROUP BY machine_name
            ORDER BY spikes DESC
            LIMIT 10;"""
    def dailyUV = """
            SELECT dt,
                   COUNT(DISTINCT client_ip)
            FROM (
              SELECT CAST(log_time AS DATE) AS dt,
                     client_ip
              FROM logs2SUFFIX
            ) AS r
            GROUP BY dt
            ORDER BY dt;"""
    def dataTransferRate = """
            SELECT AVG(transfer) / 125000000.0 AS transfer_avg,
                   MAX(transfer) / 125000000.0 AS transfer_max
            FROM (
              SELECT log_time,
                     SUM(object_size) AS transfer
              FROM logs2SUFFIX
              GROUP BY log_time
            ) AS r;"""
    def doorOpen = """
            SELECT device_name,
                   device_floor,
                   COUNT(*) AS ct
            FROM logs3SUFFIX
            WHERE event_type = 'door_open'
              AND log_time >= '2019-06-01 00:00:00.000'
            GROUP BY device_name,
                     device_floor
            ORDER BY ct DESC;"""
    def excessiveRequests = """
            SELECT client_ip,
                   COUNT(*) AS num_requests
            FROM logs2SUFFIX
            WHERE log_time >= TIMESTAMP '2012-10-01 00:00:00'
            GROUP BY client_ip
            HAVING COUNT(*) >= 100000
            ORDER BY num_requests DESC;"""
    def hourlyNetworkTraffic = """
            SELECT dt,
                   hr,
                   SUM(net_in) AS net_in_sum,
                   SUM(net_out) AS net_out_sum,
                   SUM(net_in) + SUM(net_out) AS both_sum
            FROM (
              SELECT CAST(log_time AS DATE) AS dt,
                     EXTRACT(HOUR FROM log_time) AS hr,
                     COALESCE(bytes_in, 0.0) / 1000000000.0 AS net_in,
                     COALESCE(bytes_out, 0.0) / 1000000000.0 AS net_out
              FROM logs1SUFFIX
              WHERE machine_name IN ('allsorts','andes','bigred','blackjack','bonbon',
                  'cadbury','chiclets','cotton','crows','dove','fireball','hearts','huey',
                  'lindt','milkduds','milkyway','mnm','necco','nerds','orbit','peeps',
                  'poprocks','razzles','runts','smarties','smuggler','spree','stride',
                  'tootsie','trident','wrigley','york')
            ) AS r
            GROUP BY dt,
                     hr
            ORDER BY both_sum DESC
            LIMIT 10;"""
    def lowMemory = """
            SELECT machine_name,
                   dt,
                   MIN(mem_free) AS mem_free_min
            FROM (
              SELECT machine_name,
                     CAST(log_time AS DATE) AS dt,
                     mem_free
              FROM logs1SUFFIX
              WHERE machine_group = 'DMZ'
                AND mem_free IS NOT NULL
            ) AS r
            GROUP BY machine_name,
                     dt
            HAVING MIN(mem_free) < 10000
            ORDER BY machine_name,
                     dt;"""
    def offlineMachine = """
            SELECT machine_name,
                   log_time
            FROM logs1SUFFIX
            WHERE (machine_name LIKE 'cslab%' OR
                   machine_name LIKE 'mslab%')
              AND load_one IS NULL
              AND log_time >= TIMESTAMP '2017-01-10 00:00:00'
            ORDER BY machine_name,
                     log_time;"""
    def passwordLeaked = """
            SELECT *
            FROM logs2SUFFIX
            WHERE status_code >= 200
              AND status_code < 300
              AND request LIKE '%/etc/passwd%'
              AND log_time >= TIMESTAMP '2012-05-06 00:00:00'
              AND log_time < TIMESTAMP '2012-05-20 00:00:00'
            ORDER BY log_time;"""
    def powerConsumptionMetrics = """
            SELECT yr,
                   mo,
                   SUM(coffee_hourly_avg) AS coffee_monthly_sum,
                   AVG(coffee_hourly_avg) AS coffee_monthly_avg,
                   SUM(printer_hourly_avg) AS printer_monthly_sum,
                   AVG(printer_hourly_avg) AS printer_monthly_avg,
                   SUM(projector_hourly_avg) AS projector_monthly_sum,
                   AVG(projector_hourly_avg) AS projector_monthly_avg,
                   SUM(vending_hourly_avg) AS vending_monthly_sum,
                   AVG(vending_hourly_avg) AS vending_monthly_avg
            FROM (
              SELECT dt,
                     yr,
                     mo,
                     hr,
                     AVG(coffee) AS coffee_hourly_avg,
                     AVG(printer) AS printer_hourly_avg,
                     AVG(projector) AS projector_hourly_avg,
                     AVG(vending) AS vending_hourly_avg
              FROM (
                SELECT CAST(log_time AS DATE) AS dt,
                       EXTRACT(YEAR FROM log_time) AS yr,
                       EXTRACT(MONTH FROM log_time) AS mo,
                       EXTRACT(HOUR FROM log_time) AS hr,
                       CASE WHEN device_name LIKE 'coffee%' THEN event_value END AS coffee,
                       CASE WHEN device_name LIKE 'printer%' THEN event_value END AS printer,
                       CASE WHEN device_name LIKE 'projector%' THEN event_value END AS projector,
                       CASE WHEN device_name LIKE 'vending%' THEN event_value END AS vending
                FROM logs3SUFFIX
                WHERE device_type = 'meter'
              ) AS r
              GROUP BY dt,
                       yr,
                       mo,
                       hr
            ) AS s
            GROUP BY yr,
                     mo
            ORDER BY yr,
                     mo;"""
    def serverError = """
            SELECT *
            FROM logs2SUFFIX
            WHERE status_code >= 500
              AND log_time >= TIMESTAMP '2012-12-18 00:00:00'
            ORDER BY log_time;"""
    def temperatureReachFreezing = """
            SELECT *
            FROM logs3SUFFIX
            WHERE event_type = 'temperature'
              AND event_value <= 32.0
              AND log_time >= '2019-11-29 17:00:00.000'
            ORDER BY log_time;"""
    def temperatureVariation_order = """
            WITH temperature AS (
              SELECT dt,
                     device_name,
                     device_type,
                     device_floor
              FROM (
                SELECT dt,
                       hr,
                       device_name,
                       device_type,
                       device_floor,
                       AVG(event_value) AS temperature_hourly_avg
                FROM (
                  SELECT CAST(log_time AS DATE) AS dt,
                         EXTRACT(HOUR FROM log_time) AS hr,
                         device_name,
                         device_type,
                         device_floor,
                         event_value
                  FROM logs3SUFFIX
                  WHERE event_type = 'temperature'
                ) AS r
                GROUP BY dt,
                         hr,
                         device_name,
                         device_type,
                         device_floor
              ) AS s
              GROUP BY dt,
                       device_name,
                       device_type,
                       device_floor
              HAVING MAX(temperature_hourly_avg) - MIN(temperature_hourly_avg) >= 25.0
            )
            SELECT DISTINCT device_name,
                   device_type,
                   device_floor,
                   'WINTER'
            FROM temperature
            WHERE dt >= DATE '2018-12-01'
              AND dt < DATE '2019-03-01'
            UNION
            SELECT DISTINCT device_name,
                   device_type,
                   device_floor,
                   'SUMMER'
            FROM temperature
            WHERE dt >= DATE '2019-06-01'
              AND dt < DATE '2019-09-01';"""


    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "external_brown"

        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        sql """switch ${catalog_name};"""

        logger.info("switched to catalog " + catalog_name)

        sql """use multi_catalog;"""

        logger.info("use multi_catalog")

        for (String format in formats) {
            logger.info("Process format " + format)
            qt_01 CPUNetworkUtilization_order.replace("SUFFIX", format)
            qt_02 averageMetrics.replace("SUFFIX", format)
            qt_03 blockedIOServer.replace("SUFFIX", format)
            qt_04 dailyUV.replace("SUFFIX", format)
            qt_05 dataTransferRate.replace("SUFFIX", format)
            qt_06 doorOpen.replace("SUFFIX", format)
            qt_07 excessiveRequests.replace("SUFFIX", format)
            qt_08 hourlyNetworkTraffic.replace("SUFFIX", format)
            qt_09 lowMemory.replace("SUFFIX", format)
            qt_10 offlineMachine.replace("SUFFIX", format)
            qt_11 passwordLeaked.replace("SUFFIX", format)
            qt_12 powerConsumptionMetrics.replace("SUFFIX", format)
            qt_13 serverError.replace("SUFFIX", format)
            qt_14 temperatureReachFreezing.replace("SUFFIX", format)
            order_qt_15 temperatureVariation_order.replace("SUFFIX", format)
        }
    }
}

