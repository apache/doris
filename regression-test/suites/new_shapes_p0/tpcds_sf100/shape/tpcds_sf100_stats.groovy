/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
suite('tpcds_sf100_stats') {
    if (isCloudMode()) {
        return
    }
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    def stats
    stats = sql """ show column stats call_center            ;"""
    logger.info("${stats}")
    stats = sql """ show column stats catalog_page           ;"""
    logger.info("${stats}")
    stats = sql """ show column stats catalog_returns        ;"""
    logger.info("${stats}")
    stats = sql """ show column stats catalog_sales          ;"""
    logger.info("${stats}")
    stats = sql """ show column stats customer               ;"""
    logger.info("${stats}")
    stats = sql """ show column stats customer_address       ;"""
    logger.info("${stats}")
    stats = sql """ show column stats customer_demographics  ;"""
    logger.info("${stats}")
    stats = sql """ show column stats date_dim               ;"""
    logger.info("${stats}")
    stats = sql """ show column stats dbgen_version          ;"""
    logger.info("${stats}")
    stats = sql """ show column stats household_demographics ;"""
    logger.info("${stats}")
    stats = sql """ show column stats income_band            ;"""
    logger.info("${stats}")
    stats = sql """ show column stats inventory              ;"""
    logger.info("${stats}")
    stats = sql """ show column stats item                   ;"""
    logger.info("${stats}")
    stats = sql """ show column stats promotion              ;"""
    logger.info("${stats}")
    stats = sql """ show column stats reason                 ;"""
    logger.info("${stats}")
    stats = sql """ show column stats ship_mode              ;"""
    logger.info("${stats}")
    stats = sql """ show column stats store                  ;"""
    logger.info("${stats}")
    stats = sql """ show column stats store_returns          ;"""
    logger.info("${stats}")
    stats = sql """ show column stats store_sales            ;"""
    logger.info("${stats}")
    stats = sql """ show column stats time_dim               ;"""
    logger.info("${stats}")
    stats = sql """ show column stats warehouse              ;"""
    logger.info("${stats}")
    stats = sql """ show column stats web_page               ;"""
    logger.info("${stats}")
    stats = sql """ show column stats web_returns            ;"""
    logger.info("${stats}")
    stats = sql """ show column stats web_sales              ;"""
    logger.info("${stats}")
    stats = sql """ show column stats web_site               ;"""
    logger.info("${stats}")

}