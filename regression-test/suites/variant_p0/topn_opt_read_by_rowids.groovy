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

suite("regression_test_variant_topn_opt_read_by_rowids", "p0"){
    sql "DROP TABLE IF EXISTS test_web_log"
    sql """
        CREATE TABLE `test_web_log` (
  `timestampkey` DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '',
  `nodeName` VARCHAR(100) NULL COMMENT '',
  `serviceCode` VARCHAR(100) NULL COMMENT '',
  `pageTitle` VARCHAR(100) NULL COMMENT '',
  `nodeIp` VARCHAR(100) NULL COMMENT '',
  `clientTs` LARGEINT NULL COMMENT '',
  `spanId` VARCHAR(1000) NULL COMMENT '',
  `loginName` VARCHAR(100) NULL COMMENT '',
  `host` VARCHAR(100) NULL COMMENT '',
  `nameSpace` VARCHAR(100) NULL COMMENT '',
  `event` VARIANT NULL COMMENT '',
  `httpInfo` VARIANT NULL COMMENT '',
  `iworkHost` VARCHAR(100) NULL COMMENT '',
  `traceId` VARCHAR(1000) NULL COMMENT '',
  `pSpanId` VARCHAR(1000) NULL COMMENT '',
  `timing` VARIANT NULL COMMENT '',
  `clusterFlag` VARCHAR(100) NULL COMMENT '',
  `sessionId` VARCHAR(1000) NULL COMMENT '',
  `serviceName` VARCHAR(100) NULL COMMENT '',
  `userName` VARCHAR(100) NULL COMMENT '',
  `uri` VARCHAR(1000) NULL COMMENT '',
  `userId` VARCHAR(100) NULL COMMENT '',
  `url` VARCHAR(1000) NULL COMMENT '',
  `systemCode` VARCHAR(100) NULL COMMENT '',
  `tenantId` VARCHAR(100) NULL COMMENT '',
  `podName` VARCHAR(100) NULL COMMENT '',
  `siteId` VARCHAR(100) NULL COMMENT '',
  `time` VARCHAR(100) NULL COMMENT '',
  `region` VARCHAR(100) NULL COMMENT '',
  `device` VARIANT NULL COMMENT '',
  `ts` VARCHAR(100) NULL COMMENT '',
  `timestamp` LARGEINT NULL COMMENT '',
  `resourceLoad` VARIANT NULL COMMENT '',
  `scriptError` VARIANT NULL COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(`timestampkey`)
COMMENT 'OLAP'
PARTITION BY RANGE(`timestampkey`)
(
PARTITION p2024040909 VALUES [('2024-04-09 09:00:00'), ('2024-04-09 10:00:00')),
PARTITION p2024040910 VALUES [('2024-04-09 10:00:00'), ('2024-04-09 11:00:00')),
PARTITION p2024040911 VALUES [('2024-04-09 11:00:00'), ('2024-04-09 12:00:00')),
PARTITION p2024040923 VALUES [('2024-04-09 12:00:00'), ('2024-04-10 00:00:00')))
DISTRIBUTED BY HASH(`traceId`) BUCKETS 25
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
        """
    sql """
        INSERT INTO `test_web_log` VALUES ('2024-04-09 09:01:39', '', '', '', '', '1712624474952', '0004.00', '740lp', 'sit-iniwork-lcd-designer.qm.cn', '', NULL, NULL, '', '630beed604d0513c', '0000.0', NULL, '', '', '', '', 'https://sit-iniwork-lcd-designer.qm.cn/designer', '1544512389907021826', 'https://sit-iniwork-lcd-designer.qm.cn/designer?caseName=44&caseCode=d4&appId=3fec598d32b10d26958d1d9119519c64&token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI3NDBscCIsImNyZWF0ZWQiOjE3MTI1NDEzMTU0MzUsImlkbWlkIjpudWxsLCJleHAiOjE3MTMxNDYxMTUsInVwa2lkIjoiMTU0NDUxMjM4OTkwNzAyMTgyNiJ9.j8sJbrgrJzwPh5Ee1AoodqIJ7KsMT_qfUhZlyon6rRsMClRI5WpUYpSWkC7P-axolUUnWJi2Llw89hO_KyL_gg&formId=8f4f75e0c0aa93fcd0a4eae843114527&tenantId=YQJT&systemId=202203A-015&subType=0', '', '', '', '202203A-015_APP_DES', '20240409090114952', '未知', '{\"browser\":\"Chrome 123.0.0.0\",\"browserFlag\":\"a8ba48f1849c2423\",\"browserName\":\"Chrome\",\"browserVersion\":\"123.0.0.0\",\"clientStr\":\"10.133.53.99\",\"headerClient\":\"{X-Original-Forwarded-For=10.133.53.99, X-Forwarded-For=10.140.199.9, RemoteAddr=10.133.53.99, X-Real-IP=10.140.199.9}\",\"os\":\"Windows 10\",\"osName\":\"Windows\",\"osVersion\":\"10\",\"res\":\"1920x1080\",\"type\":\"Computer\"}', '1712624474952', '1712624475992', NULL, NULL);
        """
    sql """
        INSERT INTO `test_web_log` VALUES ('2024-04-09 09:01:39', '', '', '', '', '1712624474952', '0004.00', '740lp', 'sit-iniwork-lcd-designer.qm.cn', '', NULL, NULL, '', '630beed604d0513c', '0000.0', NULL, '', '', '', '', 'https://sit-iniwork-lcd-designer.qm.cn/designer', '1544512389907021826', 'https://sit-iniwork-lcd-designer.qm.cn/designer?caseName=44&caseCode=d4&appId=3fec598d32b10d26958d1d9119519c64&token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI3NDBscCIsImNyZWF0ZWQiOjE3MTI1NDEzMTU0MzUsImlkbWlkIjpudWxsLCJleHAiOjE3MTMxNDYxMTUsInVwa2lkIjoiMTU0NDUxMjM4OTkwNzAyMTgyNiJ9.j8sJbrgrJzwPh5Ee1AoodqIJ7KsMT_qfUhZlyon6rRsMClRI5WpUYpSWkC7P-axolUUnWJi2Llw89hO_KyL_gg&formId=8f4f75e0c0aa93fcd0a4eae843114527&tenantId=YQJT&systemId=202203A-015&subType=0', '', '', '', '202203A-015_APP_DES', '20240409090114952', '未知', '{\"browser\":\"Chrome 123.0.0.0\",\"browserFlag\":\"a8ba48f1849c2423\",\"browserName\":\"Chrome\",\"browserVersion\":\"123.0.0.0\",\"clientStr\":\"10.133.53.99\",\"headerClient\":\"{X-Original-Forwarded-For=10.133.53.99, X-Forwarded-For=10.140.199.9, RemoteAddr=10.133.53.99, X-Real-IP=10.140.199.9}\",\"os\":\"Windows 10\",\"osName\":\"Windows\",\"osVersion\":\"10\",\"res\":\"1920x1080\",\"type\":\"Computer\"}', '1712624474952', '1712624475992', NULL, NULL);
        """
    sql """
        INSERT INTO `test_web_log` VALUES ('2024-04-09 09:01:39', '', '', '', '', '1712624474952', '0004.00', '740lp', 'sit-iniwork-lcd-designer.qm.cn', '', NULL, NULL, '', '630beed604d0513c', '0000.0', NULL, '', '', '', '', 'https://sit-iniwork-lcd-designer.qm.cn/designer', '1544512389907021826', 'https://sit-iniwork-lcd-designer.qm.cn/designer?caseName=44&caseCode=d4&appId=3fec598d32b10d26958d1d9119519c64&token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI3NDBscCIsImNyZWF0ZWQiOjE3MTI1NDEzMTU0MzUsImlkbWlkIjpudWxsLCJleHAiOjE3MTMxNDYxMTUsInVwa2lkIjoiMTU0NDUxMjM4OTkwNzAyMTgyNiJ9.j8sJbrgrJzwPh5Ee1AoodqIJ7KsMT_qfUhZlyon6rRsMClRI5WpUYpSWkC7P-axolUUnWJi2Llw89hO_KyL_gg&formId=8f4f75e0c0aa93fcd0a4eae843114527&tenantId=YQJT&systemId=202203A-015&subType=0', '', '', '', '202203A-015_APP_DES', '20240409090114952', '未知', '{\"browser\":\"Chrome 123.0.0.0\",\"browserFlag\":\"a8ba48f1849c2423\",\"browserName\":\"Chrome\",\"browserVersion\":\"123.0.0.0\",\"clientStr\":\"10.133.53.99\",\"headerClient\":\"{X-Original-Forwarded-For=10.133.53.99, X-Forwarded-For=10.140.199.9, RemoteAddr=10.133.53.99, X-Real-IP=10.140.199.9}\",\"os\":\"Windows 10\",\"osName\":\"Windows\",\"osVersion\":\"10\",\"res\":\"1920x1080\",\"type\":\"Computer\"}', '1712624474952', '1712624475992', NULL, NULL);
        """
    sql "set topn_opt_limit_threshold = 1024"
    qt_sql """SELECT
            * FROM
             test_web_log 
            WHERE
            ts  >= '1712480940849' 
             AND ts  <= '1712805483291'
            ORDER BY
             ts DESC 
             LIMIT 10"""
    sql """
        INSERT INTO `test_web_log` VALUES ('2024-04-09 09:02:31', '', '', '', '', '1712624495211', '004.00', '740lp', 'sit-iniwork-lcd-designer.qm.cn', '', NULL, NULL, '', '630beed604d0513c', '0000.0', NULL, '', '', '', '', 'https://sit-iniwork-lcd-designer.qm.cn/designer', '1544512389907021826', 'https://sit-iniwork-lcd-designer.qm.cn/designer?caseName=44&caseCode=d4&appId=3fec598d32b10d26958d1d9119519c64&token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI3NDBscCIsImNyZWF0ZWQiOjE3MTI1NDEzMTU0MzUsImlkbWlkIjpudWxsLCJleHAiOjE3MTMxNDYxMTUsInVwa2lkIjoiMTU0NDUxMjM4OTkwNzAyMTgyNiJ9.j8sJbrgrJzwPh5Ee1AoodqIJ7KsMT_qfUhZlyon6rRsMClRI5WpUYpSWkC7P-axolUUnWJi2Llw89hO_KyL_gg&formId=8f4f75e0c0aa93fcd0a4eae843114527&tenantId=YQJT&systemId=202203A-015&subType=0', '', '', '', '202203A-015_APP_DES', '20240409090114952', '未知', '{\"browser1\":\"Chrome 123.0.0.0\",\"browserFlag1\":\"a8ba48f1849c2423\",\"browserName4\":\"Chrome\",\"browserVersion7\":\"123.0.0.0\",\"clientStr\":\"10.133.53.99\",\"headerClient\":\"{X-Original-Forwarded-For=10.033.53.19, X-Forwarded-For=10.140.199.9, RemoteAddr=10.133.53.99, X-Real-IP=10.140.199.9}\",\"os\":\"Windows 10\",\"osName\":\"Windows\",\"osVersion\":\"10\",\"res\":\"1920x1080\",\"type\":\"Computer\"}', '1712624474952', '1712624475992', NULL, NULL);
        """
    sql """
        INSERT INTO `test_web_log` VALUES ('2024-04-09 09:03:32', '', '', '', '', '1712624474153', '0014.00', '740lp', 'sit-iniwork-lcd-designer.qm.cn', '', NULL, NULL, '', '630beed604d0513c', '0000.0', NULL, '', '', '', '', 'https://sit-iniwork-lcd-designer.qm.cn/designer', '1544512389907021826', 'https://sit-iniwork-lcd-designer.qm.cn/designer?caseName=44&caseCode=d4&appId=3fec598d32b10d26958d1d9119519c64&token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI3NDBscCIsImNyZWF0ZWQiOjE3MTI1NDEzMTU0MzUsImlkbWlkIjpudWxsLCJleHAiOjE3MTMxNDYxMTUsInVwa2lkIjoiMTU0NDUxMjM4OTkwNzAyMTgyNiJ9.j8sJbrgrJzwPh5Ee1AoodqIJ7KsMT_qfUhZlyon6rRsMClRI5WpUYpSWkC7P-axolUUnWJi2Llw89hO_KyL_gg&formId=8f4f75e0c0aa93fcd0a4eae843114527&tenantId=YQJT&systemId=202203A-015&subType=0', '', '', '', '202203A-015_APP_DES', '20240409090114952', '未知', '{\"browser2\":\"Chrome 123.0.0.0\",\"browserFlag2\":\"a8ba48f1849c2423\",\"browserName5\":\"Chrome\",\"browserVersion8\":\"123.0.0.0\",\"clientStr\":\"10.133.53.99\",\"headerClient\":\"{X-Original-Forwarded-For=10.133.53.99, X-Forwarded-For=10.140.199.9, RemoteAddr=10.133.53.99, X-Real-IP=10.140.199.9}\",\"os\":\"Windows 10\",\"osName\":\"Windows\",\"osVersion\":\"10\",\"res\":\"1920x1080\",\"type\":\"Computer\"}', '1712624474952', '1712624475992', NULL, NULL);
        """
    sql """
        INSERT INTO `test_web_log` VALUES ('2024-04-09 09:04:33', '', '', '', '', '1712624474959', '0024.00', '740lp', 'sit-iniwork-lcd-designer.qm.cn', '', NULL, NULL, '', '630beed604d0513c', '0000.0', NULL, '', '', '', '', 'https://sit-iniwork-lcd-designer.qm.cn/designer', '1544512389907021826', 'https://sit-iniwork-lcd-designer.qm.cn/designer?caseName=44&caseCode=d4&appId=3fec598d32b10d26958d1d9119519c64&token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI3NDBscCIsImNyZWF0ZWQiOjE3MTI1NDEzMTU0MzUsImlkbWlkIjpudWxsLCJleHAiOjE3MTMxNDYxMTUsInVwa2lkIjoiMTU0NDUxMjM4OTkwNzAyMTgyNiJ9.j8sJbrgrJzwPh5Ee1AoodqIJ7KsMT_qfUhZlyon6rRsMClRI5WpUYpSWkC7P-axolUUnWJi2Llw89hO_KyL_gg&formId=8f4f75e0c0aa93fcd0a4eae843114527&tenantId=YQJT&systemId=202203A-015&subType=0', '', '', '', '202203A-015_APP_DES', '20240409090114952', '未知', '{\"browser3\":\"Chrome 123.0.0.0\",\"browserFlag3\":\"a8ba48f1849c2423\",\"browserName6\":\"Chrome\",\"browserVersion9\":\"123.0.0.0\",\"clientStr\":\"10.133.53.99\",\"headerClient\":\"{X-Original-Forwarded-For=10.133.53.99, X-Forwarded-For=10.140.199.9, RemoteAddr=10.133.53.99, X-Real-IP=10.140.199.9}\",\"os\":\"Windows 10\",\"osName\":\"Windows\",\"osVersion\":\"10\",\"res\":\"1920x1080\",\"type\":\"Computer\"}', '1712624474952', '1712624475992', NULL, NULL);
        """
    qt_sql """SELECT
            * FROM
             test_web_log 
            ORDER BY
             ts DESC 
             LIMIT 10"""
} 