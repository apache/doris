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

import groovy.json.JsonSlurper

suite('q01_trans_debug', 'p0') {
    def httpGet = { url ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + url).openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        conn.setRequestProperty("Cache-Control", "no-cache")
        conn.setRequestProperty("Pragma", "no-cache")
        conn.setConnectTimeout(10000) // 10 seconds
        conn.setReadTimeout(10000) // 10 seconds

        int responseCode = conn.getResponseCode()
        log.info("HTTP response status: " + responseCode)

        if (responseCode == 200) {
            InputStream inputStream = conn.getInputStream()
            String response = inputStream.text
            inputStream.close()
            return response
        } else {
            log.error("HTTP request failed with response code: " + responseCode)
            return null
        }
    }

    def getProfileIdWithRetry = { query, maxRetries, waitSeconds ->
        def profileUrl = '/rest/v1/query_profile/'
        def profiles = null
        def profileId = null
        int attempt = 0

        while (attempt < maxRetries) {
            sql "sync"
            sql """ ${query} """
            profiles = httpGet(profileUrl)
            log.info("profiles attempt ${attempt + 1}: {}", profiles)
            if (profiles == null) {
                log.warn("Failed to fetch profiles on attempt ${attempt + 1}")
            } else {
                def jsonProfiles = new JsonSlurper().parseText(profiles)
                if (jsonProfiles.code == 0) {
                    for (def profile in jsonProfiles["data"]["rows"]) {
                        if (profile["Sql Statement"].contains(query)) {
                            profileId = profile["Profile ID"]
                            break
                        }
                    }
                } else {
                    log.warn("Profile response code is not 0 on attempt ${attempt + 1}")
                }
            }

            if (profileId != null) {
                break
            } else {
                attempt++
                if (attempt < maxRetries) {
                    log.info("profileId is null, retrying after ${waitSeconds} second(s)... (Attempt ${attempt + 1}/${maxRetries})")
                    sleep(waitSeconds * 1000)
                }
            }
        }

        assertTrue(profileId != null, "Failed to retrieve profileId after ${maxRetries} attempts")
        return profileId
    }

    
    def query = """
    SELECT  /*+SET_VAR(enable_fallback_to_original_planner=false) */
  CAST(var["L_RETURNFLAG"] AS TEXT),
  CAST(var["L_LINESTATUS"] AS TEXT),
  SUM(CAST(var["L_QUANTITY"] AS DOUBLE))                                       AS SUM_QTY,
  SUM(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE))                                  AS SUM_BASE_PRICE,
  SUM(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(var["L_DISCOUNT"] AS DOUBLE)))               AS SUM_DISC_PRICE,
  SUM(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE) * (1 - CAST(var["L_DISCOUNT"] AS DOUBLE)) * (1 + CAST(var["L_TAX"] AS DOUBLE))) AS SUM_CHARGE,
  AVG(CAST(var["L_QUANTITY"] AS DOUBLE))                                       AS AVG_QTY,
  AVG(CAST(var["L_EXTENDEDPRICE"] AS DOUBLE))                                  AS AVG_PRICE,
  AVG(CAST(var["L_DISCOUNT"] AS DOUBLE))                                       AS AVG_DISC,
  COUNT(*)                                              AS COUNT_ORDER
FROM
  lineitem
WHERE
  CAST(var["L_SHIPDATE"] AS DATE) <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY
CAST(var["L_RETURNFLAG"] AS TEXT),
CAST(var["L_LINESTATUS"] AS TEXT)
ORDER BY
CAST(var["L_RETURNFLAG"] AS TEXT),
CAST(var["L_LINESTATUS"] AS TEXT)
     """


    sql "set enable_profile = true"
    try {
        def profid = getProfileIdWithRetry(query, 5, 100)
        logger.info("debug profile id: ${profid}")
    } finally {
        sql "set enable_profile = false"
    }
}
