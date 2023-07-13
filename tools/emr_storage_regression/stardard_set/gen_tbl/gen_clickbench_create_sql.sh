#!/bin/bash
if [ -z $1 ]; then
    echo 'the first argument is database location'
    exit
else 
    db_loc=$1
fi

if [ -z $2 ]; then
    echo 'the second argument is database name'
    exit
else
    db=$2
fi
if [ -z $3 ]; then
    format=parquet
else
    format=$3
fi

echo '
CREATE DATABASE  IF NOT EXISTS '"${db}"';
USE '"${db}"';
CREATE TABLE IF NOT EXISTS `hits`(
  `WatchID` BIGINT,
  `JavaEnable` SMALLINT,
  `Title` STRING,
  `GoodEvent` SMALLINT,
  `EventTime` TIMESTAMP,
  `EventDate` DATE,
  `CounterID` INT,
  `ClientIP` INT,
  `RegionID` INT,
  `UserID` BIGINT,
  `CounterClass` SMALLINT,
  `OS` SMALLINT,
  `UserAgent` SMALLINT,
  `URL` STRING,
  `Referer` STRING,
  `IsRefresh` SMALLINT,
  `RefererCategoryID` SMALLINT,
  `RefererRegionID` INT,
  `URLCategoryID` SMALLINT,
  `URLRegionID` INT,
  `ResolutionWidth` SMALLINT,
  `ResolutionHeight` SMALLINT,
  `ResolutionDepth` SMALLINT,
  `FlashMajor` SMALLINT,
  `FlashMinor` SMALLINT,
  `FlashMinor2` STRING,
  `NetMajor` SMALLINT,
  `NetMinor` SMALLINT,
  `UserAgentMajor` SMALLINT,
  `UserAgentMinor` STRING,
  `CookieEnable` SMALLINT,
  `JavascriptEnable` SMALLINT,
  `IsMobile` SMALLINT,
  `MobilePhone` SMALLINT,
  `MobilePhoneModel` STRING,
  `Params` STRING,
  `IPNetworkID` INT,
  `TraficSourceID` SMALLINT,
  `SearchEngineID` SMALLINT,
  `SearchPhrase` STRING,
  `AdvEngineID` SMALLINT,
  `IsArtifical` SMALLINT,
  `WindowClientWidth` SMALLINT,
  `WindowClientHeight` SMALLINT,
  `ClientTimeZone` SMALLINT,
  `ClientEventTime` TIMESTAMP,
  `SilverlightVersion1` SMALLINT,
  `SilverlightVersion2` SMALLINT,
  `SilverlightVersion3` INT,
  `SilverlightVersion4` SMALLINT,
  `PageCharset` STRING,
  `CodeVersion` INT,
  `IsLink` SMALLINT,
  `IsDownload` SMALLINT,
  `IsNotBounce` SMALLINT,
  `FUniqID` BIGINT,
  `OriginalURL` STRING,
  `HID` INT,
  `IsOldCounter` SMALLINT,
  `IsEvent` SMALLINT,
  `IsParameter` SMALLINT,
  `DontCountHits` SMALLINT,
  `WithHash` SMALLINT,
  `HitColor` STRING,
  `LocalEventTime` TIMESTAMP,
  `Age` SMALLINT,
  `Sex` SMALLINT,
  `Income` SMALLINT,
  `Interests` SMALLINT,
  `Robotness` SMALLINT,
  `RemoteIP` INT,
  `WindowName` INT,
  `OpenerName` INT,
  `HistoryLength` SMALLINT,
  `BrowserLanguage` STRING,
  `BrowserCountry` STRING,
  `SocialNetwork` STRING,
  `SocialAction` STRING,
  `HTTPError` SMALLINT,
  `SendTiming` INT,
  `DNSTiming` INT,
  `ConnectTiming` INT,
  `ResponseStartTiming` INT,
  `ResponseEndTiming` INT,
  `FetchTiming` INT,
  `SocialSourceNetworkID` SMALLINT,
  `SocialSourcePage` STRING,
  `ParamPrice` BIGINT,
  `ParamOrderID` STRING,
  `ParamCurrency` STRING,
  `ParamCurrencyID` SMALLINT,
  `OpenstatServiceName` STRING,
  `OpenstatCampaignID` STRING,
  `OpenstatAdID` STRING,
  `OpenstatSourceID` STRING,
  `UTMSource` STRING,
  `UTMMedium` STRING,
  `UTMCampaign` STRING,
  `UTMContent` STRING,
  `UTMTerm` STRING,
  `FromTag` STRING,
  `HasGCLID` SMALLINT,
  `RefererHash` BIGINT,
  `URLHash` BIGINT,
  `CLID` INT)
USING '"${format}"'
LOCATION "'"${db_loc}"'";
'