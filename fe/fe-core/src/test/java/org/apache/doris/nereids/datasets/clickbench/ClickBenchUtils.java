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

package org.apache.doris.nereids.datasets.clickbench;

import org.apache.doris.utframe.TestWithFeService;

public class ClickBenchUtils {

    public static final String Q0 = "SELECT COUNT(*) FROM hits;";
    public static final String Q1 = "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;";
    public static final String Q2 = "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;";
    public static final String Q3 = "SELECT AVG(UserID) FROM hits;";
    public static final String Q4 = "SELECT COUNT(DISTINCT UserID) FROM hits;";
    public static final String Q5 = "SELECT COUNT(DISTINCT SearchPhrase) FROM hits;";
    public static final String Q6 = "SELECT MIN(EventDate), MAX(EventDate) FROM hits;";
    public static final String Q7 = "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;";
    public static final String Q8 = "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;";
    public static final String Q9 = "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;";
    public static final String Q10 = "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;";
    public static final String Q11 = "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;";
    public static final String Q12 = "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;";
    public static final String Q13 = "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;";
    public static final String Q14 = "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;";
    public static final String Q15 = "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;";
    public static final String Q16 = "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;";
    public static final String Q17 = "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;";
    public static final String Q18 = "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;";
    public static final String Q19 = "SELECT UserID FROM hits WHERE UserID = 435090932899640449;";
    public static final String Q20 = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';";
    public static final String Q21 = "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;";
    public static final String Q22 = "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;";
    public static final String Q23 = "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;";
    public static final String Q24 = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;";
    public static final String Q25 = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;";
    public static final String Q26 = "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;";
    public static final String Q27 = "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;";
    public static final String Q28 = "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;";
    public static final String Q29 = "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;";
    public static final String Q30 = "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;";
    public static final String Q31 = "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;";
    public static final String Q32 = "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;";
    public static final String Q33 = "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;";
    public static final String Q34 = "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;";
    public static final String Q35 = "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;";
    public static final String Q36 = "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;";
    public static final String Q37 = "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;";
    public static final String Q38 = "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;";
    public static final String Q39 = "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;";
    public static final String Q40 = "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;";
    public static final String Q41 = "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;";
    public static final String Q42 = "SELECT DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') ORDER BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') LIMIT 10 OFFSET 1000;";

    public static void createTables(TestWithFeService service) throws Exception {
        service.createTable("CREATE TABLE IF NOT EXISTS hits (\n"
                + "CounterID INT NOT NULL,\n"
                + "EventDate DATEV2 NOT NULL,\n"
                + "UserID BIGINT NOT NULL,\n"
                + "EventTime DATETIMEV2 NOT NULL,\n"
                + "WatchID BIGINT NOT NULL,\n"
                + "JavaEnable SMALLINT NOT NULL,\n"
                + "Title STRING NOT NULL,\n"
                + "GoodEvent SMALLINT NOT NULL,\n"
                + "ClientIP INT NOT NULL,\n"
                + "RegionID INT NOT NULL,\n"
                + "CounterClass SMALLINT NOT NULL,\n"
                + "OS SMALLINT NOT NULL,\n"
                + "UserAgent SMALLINT NOT NULL,\n"
                + "URL STRING NOT NULL,\n"
                + "Referer STRING NOT NULL,\n"
                + "IsRefresh SMALLINT NOT NULL,\n"
                + "RefererCategoryID SMALLINT NOT NULL,\n"
                + "RefererRegionID INT NOT NULL,\n"
                + "URLCategoryID SMALLINT NOT NULL,\n"
                + "URLRegionID INT NOT NULL,\n"
                + "ResolutionWidth SMALLINT NOT NULL,\n"
                + "ResolutionHeight SMALLINT NOT NULL,\n"
                + "ResolutionDepth SMALLINT NOT NULL,\n"
                + "FlashMajor SMALLINT NOT NULL,\n"
                + "FlashMinor SMALLINT NOT NULL,\n"
                + "FlashMinor2 STRING NOT NULL,\n"
                + "NetMajor SMALLINT NOT NULL,\n"
                + "NetMinor SMALLINT NOT NULL,\n"
                + "UserAgentMajor SMALLINT NOT NULL,\n"
                + "UserAgentMinor VARCHAR(255) NOT NULL,\n"
                + "CookieEnable SMALLINT NOT NULL,\n"
                + "JavascriptEnable SMALLINT NOT NULL,\n"
                + "IsMobile SMALLINT NOT NULL,\n"
                + "MobilePhone SMALLINT NOT NULL,\n"
                + "MobilePhoneModel STRING NOT NULL,\n"
                + "Params STRING NOT NULL,\n"
                + "IPNetworkID INT NOT NULL,\n"
                + "TraficSourceID SMALLINT NOT NULL,\n"
                + "SearchEngineID SMALLINT NOT NULL,\n"
                + "SearchPhrase STRING NOT NULL,\n"
                + "AdvEngineID SMALLINT NOT NULL,\n"
                + "IsArtifical SMALLINT NOT NULL,\n"
                + "WindowClientWidth SMALLINT NOT NULL,\n"
                + "WindowClientHeight SMALLINT NOT NULL,\n"
                + "ClientTimeZone SMALLINT NOT NULL,\n"
                + "ClientEventTime DATETIMEV2 NOT NULL,\n"
                + "SilverlightVersion1 SMALLINT NOT NULL,\n"
                + "SilverlightVersion2 SMALLINT NOT NULL,\n"
                + "SilverlightVersion3 INT NOT NULL,\n"
                + "SilverlightVersion4 SMALLINT NOT NULL,\n"
                + "PageCharset STRING NOT NULL,\n"
                + "CodeVersion INT NOT NULL,\n"
                + "IsLink SMALLINT NOT NULL,\n"
                + "IsDownload SMALLINT NOT NULL,\n"
                + "IsNotBounce SMALLINT NOT NULL,\n"
                + "FUniqID BIGINT NOT NULL,\n"
                + "OriginalURL STRING NOT NULL,\n"
                + "HID INT NOT NULL,\n"
                + "IsOldCounter SMALLINT NOT NULL,\n"
                + "IsEvent SMALLINT NOT NULL,\n"
                + "IsParameter SMALLINT NOT NULL,\n"
                + "DontCountHits SMALLINT NOT NULL,\n"
                + "WithHash SMALLINT NOT NULL,\n"
                + "HitColor CHAR NOT NULL,\n"
                + "LocalEventTime DATETIMEV2 NOT NULL,\n"
                + "Age SMALLINT NOT NULL,\n"
                + "Sex SMALLINT NOT NULL,\n"
                + "Income SMALLINT NOT NULL,\n"
                + "Interests SMALLINT NOT NULL,\n"
                + "Robotness SMALLINT NOT NULL,\n"
                + "RemoteIP INT NOT NULL,\n"
                + "WindowName INT NOT NULL,\n"
                + "OpenerName INT NOT NULL,\n"
                + "HistoryLength SMALLINT NOT NULL,\n"
                + "BrowserLanguage STRING NOT NULL,\n"
                + "BrowserCountry STRING NOT NULL,\n"
                + "SocialNetwork STRING NOT NULL,\n"
                + "SocialAction STRING NOT NULL,\n"
                + "HTTPError SMALLINT NOT NULL,\n"
                + "SendTiming INT NOT NULL,\n"
                + "DNSTiming INT NOT NULL,\n"
                + "ConnectTiming INT NOT NULL,\n"
                + "ResponseStartTiming INT NOT NULL,\n"
                + "ResponseEndTiming INT NOT NULL,\n"
                + "FetchTiming INT NOT NULL,\n"
                + "SocialSourceNetworkID SMALLINT NOT NULL,\n"
                + "SocialSourcePage STRING NOT NULL,\n"
                + "ParamPrice BIGINT NOT NULL,\n"
                + "ParamOrderID STRING NOT NULL,\n"
                + "ParamCurrency STRING NOT NULL,\n"
                + "ParamCurrencyID SMALLINT NOT NULL,\n"
                + "OpenstatServiceName STRING NOT NULL,\n"
                + "OpenstatCampaignID STRING NOT NULL,\n"
                + "OpenstatAdID STRING NOT NULL,\n"
                + "OpenstatSourceID STRING NOT NULL,\n"
                + "UTMSource STRING NOT NULL,\n"
                + "UTMMedium STRING NOT NULL,\n"
                + "UTMCampaign STRING NOT NULL,\n"
                + "UTMContent STRING NOT NULL,\n"
                + "UTMTerm STRING NOT NULL,\n"
                + "FromTag STRING NOT NULL,\n"
                + "HasGCLID SMALLINT NOT NULL,\n"
                + "RefererHash BIGINT NOT NULL,\n"
                + "URLHash BIGINT NOT NULL,\n"
                + "CLID INT NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID)\n"
                + "DISTRIBUTED BY HASH(UserID) BUCKETS 48\n"
                + "PROPERTIES ( \"replication_num\"=\"1\");"
        );
    }
}
