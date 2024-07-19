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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

// syntax error:
// q06 q13 q15
// Test 23 suites, failed 3 suites

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq
suite("load") {
    if (isCloudMode()) {
        return
    }

    sql """
        DROP TABLE IF EXISTS hits
    """

    sql """
        CREATE TABLE IF NOT EXISTS hits (
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
}
