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

// This suit test the `backends` information_schema table
suite("explain_clickbench_benchmark") {
    sql """CREATE TABLE IF NOT EXISTS hits (
            CounterID INT NOT NULL, 
            EventDate Datev2 NOT NULL,
            UserID BIGINT NOT NULL, 
            EventTime DateTimev2 NOT NULL,
            WatchID BIGINT NOT NULL, 
            JavaEnable SMALLINT NOT NULL,
            Title STRING NOT NULL,
            GoodEvent SMALLINT NOT NULL,
            ClientIP INT NOT NULL,
            RegionID INT NOT NULL,
            CounterClass SMALLINT NOT NULL,
            OS SMALLINT NOT NULL,
            UserAgent SMALLINT NOT NULL,
            URL STRING NOT NULL,
            Referer STRING NOT NULL,
            IsRefresh SMALLINT NOT NULL,
            RefererCategoryID SMALLINT NOT NULL,
            RefererRegionID INT NOT NULL,
            URLCategoryID SMALLINT NOT NULL,
            URLRegionID INT NOT NULL,
            ResolutionWidth SMALLINT NOT NULL,
            ResolutionHeight SMALLINT NOT NULL,
            ResolutionDepth SMALLINT NOT NULL,
            FlashMajor SMALLINT NOT NULL,
            FlashMinor SMALLINT NOT NULL,
            FlashMinor2 STRING NOT NULL,
            NetMajor SMALLINT NOT NULL,
            NetMinor SMALLINT NOT NULL,
            UserAgentMajor SMALLINT NOT NULL,
            UserAgentMinor VARCHAR(255) NOT NULL,
            CookieEnable SMALLINT NOT NULL,
            JavascriptEnable SMALLINT NOT NULL,
            IsMobile SMALLINT NOT NULL,
            MobilePhone SMALLINT NOT NULL,
            MobilePhoneModel STRING NOT NULL,
            Params STRING NOT NULL,
            IPNetworkID INT NOT NULL,
            TraficSourceID SMALLINT NOT NULL,
            SearchEngineID SMALLINT NOT NULL,
            SearchPhrase STRING NOT NULL,
            AdvEngineID SMALLINT NOT NULL,
            IsArtifical SMALLINT NOT NULL,
            WindowClientWidth SMALLINT NOT NULL,
            WindowClientHeight SMALLINT NOT NULL,
            ClientTimeZone SMALLINT NOT NULL,
            ClientEventTime DateTimev2 NOT NULL,
            SilverlightVersion1 SMALLINT NOT NULL,
            SilverlightVersion2 SMALLINT NOT NULL,
            SilverlightVersion3 INT NOT NULL,
            SilverlightVersion4 SMALLINT NOT NULL,
            PageCharset STRING NOT NULL,
            CodeVersion INT NOT NULL,
            IsLink SMALLINT NOT NULL,
            IsDownload SMALLINT NOT NULL,
            IsNotBounce SMALLINT NOT NULL,
            FUniqID BIGINT NOT NULL,
            OriginalURL STRING NOT NULL,
            HID INT NOT NULL,
            IsOldCounter SMALLINT NOT NULL,
            IsEvent SMALLINT NOT NULL,
            IsParameter SMALLINT NOT NULL,
            DontCountHits SMALLINT NOT NULL,
            WithHash SMALLINT NOT NULL,
            HitColor CHAR NOT NULL,
            LocalEventTime DateTimev2 NOT NULL,
            Age SMALLINT NOT NULL,
            Sex SMALLINT NOT NULL,
            Income SMALLINT NOT NULL,
            Interests SMALLINT NOT NULL,
            Robotness SMALLINT NOT NULL,
            RemoteIP INT NOT NULL,
            WindowName INT NOT NULL,
            OpenerName INT NOT NULL,
            HistoryLength SMALLINT NOT NULL,
            BrowserLanguage STRING NOT NULL,
            BrowserCountry STRING NOT NULL,
            SocialNetwork STRING NOT NULL,
            SocialAction STRING NOT NULL,
            HTTPError SMALLINT NOT NULL,
            SendTiming INT NOT NULL,
            DNSTiming INT NOT NULL,
            ConnectTiming INT NOT NULL,
            ResponseStartTiming INT NOT NULL,
            ResponseEndTiming INT NOT NULL,
            FetchTiming INT NOT NULL,
            SocialSourceNetworkID SMALLINT NOT NULL,
            SocialSourcePage STRING NOT NULL,
            ParamPrice BIGINT NOT NULL,
            ParamOrderID STRING NOT NULL,
            ParamCurrency STRING NOT NULL,
            ParamCurrencyID SMALLINT NOT NULL,
            OpenstatServiceName STRING NOT NULL,
            OpenstatCampaignID STRING NOT NULL,
            OpenstatAdID STRING NOT NULL,
            OpenstatSourceID STRING NOT NULL,
            UTMSource STRING NOT NULL,
            UTMMedium STRING NOT NULL,
            UTMCampaign STRING NOT NULL,
            UTMContent STRING NOT NULL,
            UTMTerm STRING NOT NULL,
            FromTag STRING NOT NULL,
            HasGCLID SMALLINT NOT NULL,
            RefererHash BIGINT NOT NULL,
            URLHash BIGINT NOT NULL,
            CLID INT NOT NULL
        )  
        DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
        DISTRIBUTED BY HASH(UserID) BUCKETS 48
        PROPERTIES ( "replication_num"="1");
    """

    def queries = """
            SELECT COUNT(*) FROM hits;
            SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
            SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
            SELECT AVG(UserID) FROM hits;
            SELECT COUNT(DISTINCT UserID) FROM hits;
            SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
            SELECT MIN(EventDate), MAX(EventDate) FROM hits;
            SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
            SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
            SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
            SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
            SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
            SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
            SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
            SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
            SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
            SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
            SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
            SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
            SELECT UserID FROM hits WHERE UserID = 435090932899640449;
            SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
            SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
            SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
            SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
            SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;
            SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;
            SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;
            SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
            SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*\$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
            SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;
            SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
            SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
            SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
            SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
            SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
            SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
            SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
            SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
            SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
            SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
            SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;
            SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;
            SELECT DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') ORDER BY DATE_FORMAT(EventTime, '%Y-%m-%d %H:%i:00') LIMIT 10 OFFSET 1000;
            """
            .split(";\n")
            .collect {it.trim() }
            .findAll{!it.isEmpty() }
            .collect{ "explain ${it}".toString()}

    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_dphyp_optimizer=true"

    benchmark {
        warmUp true
        executeTimes 300
        skipFailure false
        printResult true

        sqls queries
    }
}
