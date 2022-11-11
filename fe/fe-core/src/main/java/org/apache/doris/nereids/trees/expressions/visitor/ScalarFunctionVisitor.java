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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Acos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AesDecrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AesEncrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AppendTrailingCharIfAbsent;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ascii;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Asin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Atan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Bin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitLength;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAnd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNotCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapFromString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapHasAll;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapHasAny;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapHash64;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapMax;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapMin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOrCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapSubsetInRange;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapSubsetLimit;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapToString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXorCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cbrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ceil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ceiling;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CharLength;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CharacterLength;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConcatWs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Conv;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTz;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Cos;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Curtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateV2;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Day;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayName;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfMonth;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfWeek;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfYear;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dceil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Degrees;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dexp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dfloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dlog1;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dlog10;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dpow;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dround;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Dsqrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.E;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Elt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EndsWith;
import org.apache.doris.nereids.trees.expressions.functions.scalar.EsQuery;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Exp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ExtractUrlParameter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FindInSet;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Fmod;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Fpow;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromBase64;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromDays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GetJsonDouble;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GetJsonInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GetJsonString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Greatest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Hex;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllCardinality;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HllHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Hour;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HourFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Initcap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Instr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonArray;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonObject;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonQuote;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExistsPath;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractBigint;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractBool;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractDouble;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractInt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractIsnull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbExtractString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParse;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseErrorToInvalid;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseErrorToNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseErrorToValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNotnull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNotnullErrorToInvalid;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNotnullErrorToValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNullableErrorToInvalid;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNullableErrorToNull;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbParseNullableErrorToValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonbType;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Length;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.expressions.functions.scalar.LocalTime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.LocalTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Locate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log10;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Log2;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lower;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lpad;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ltrim;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MakeDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Md5;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Md5Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Minute;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinuteFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MinutesDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MoneyFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Month;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthName;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash332;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash364;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Negative;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NotNullOrEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullOrEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ParseUrl;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pi;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pmod;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Positive;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Pow;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Power;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuantilePercent;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Quarter;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Radians;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpExtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Repeat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Replace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Reverse;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Right;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Rpad;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Rtrim;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Second;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SecondsDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sign;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sin;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sleep;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sm3;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sm3sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sm4Decrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sm4Encrypt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Space;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SplitPart;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sqrt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StAstext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StAswkt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StCircle;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StContains;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StDistanceSphere;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StGeometryfromtext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StGeomfromtext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StLinefromtext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StLinestringfromtext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StPoint;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StPolyfromtext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StPolygon;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StPolygonfromtext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StX;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StY;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StartsWith;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrLeft;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrRight;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SubBitmap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Tan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.TimeDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Timestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBase64;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmap;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToBitmapWithCheck;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDateV2;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToDays;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToQuantileState;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Trim;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Upper;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UtcTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Version;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Week;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeekOfYear;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Weekday;
import org.apache.doris.nereids.trees.expressions.functions.scalar.WeeksDiff;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearCeil;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearFloor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearWeek;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsDiff;

/** ScalarFunctionVisitor. */
public interface ScalarFunctionVisitor<R, C> {

    R visitScalarFunction(ScalarFunction scalarFunction, C context);

    default R visitAbs(Abs abs, C context) {
        return visitScalarFunction(abs, context);
    }

    default R visitAcos(Acos acos, C context) {
        return visitScalarFunction(acos, context);
    }

    default R visitAesDecrypt(AesDecrypt aesDecrypt, C context) {
        return visitScalarFunction(aesDecrypt, context);
    }

    default R visitAesEncrypt(AesEncrypt aesEncrypt, C context) {
        return visitScalarFunction(aesEncrypt, context);
    }

    default R visitAppendTrailingCharIfAbsent(AppendTrailingCharIfAbsent function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitAscii(Ascii ascii, C context) {
        return visitScalarFunction(ascii, context);
    }

    default R visitAsin(Asin asin, C context) {
        return visitScalarFunction(asin, context);
    }

    default R visitAtan(Atan atan, C context) {
        return visitScalarFunction(atan, context);
    }

    default R visitBin(Bin bin, C context) {
        return visitScalarFunction(bin, context);
    }

    default R visitBitLength(BitLength bitLength, C context) {
        return visitScalarFunction(bitLength, context);
    }

    default R visitBitmapAnd(BitmapAnd bitmapAnd, C context) {
        return visitScalarFunction(bitmapAnd, context);
    }

    default R visitBitmapAndCount(BitmapAndCount bitmapAndCount, C context) {
        return visitScalarFunction(bitmapAndCount, context);
    }

    default R visitBitmapAndNot(BitmapAndNot bitmapAndNot, C context) {
        return visitScalarFunction(bitmapAndNot, context);
    }

    default R visitBitmapAndNotCount(BitmapAndNotCount bitmapAndNotCount, C context) {
        return visitScalarFunction(bitmapAndNotCount, context);
    }

    default R visitBitmapContains(BitmapContains bitmapContains, C context) {
        return visitScalarFunction(bitmapContains, context);
    }

    default R visitBitmapCount(BitmapCount bitmapCount, C context) {
        return visitScalarFunction(bitmapCount, context);
    }

    default R visitBitmapEmpty(BitmapEmpty bitmapEmpty, C context) {
        return visitScalarFunction(bitmapEmpty, context);
    }

    default R visitBitmapFromString(BitmapFromString bitmapFromString, C context) {
        return visitScalarFunction(bitmapFromString, context);
    }

    default R visitBitmapHasAll(BitmapHasAll bitmapHasAll, C context) {
        return visitScalarFunction(bitmapHasAll, context);
    }

    default R visitBitmapHasAny(BitmapHasAny bitmapHasAny, C context) {
        return visitScalarFunction(bitmapHasAny, context);
    }

    default R visitBitmapHash(BitmapHash bitmapHash, C context) {
        return visitScalarFunction(bitmapHash, context);
    }

    default R visitBitmapHash64(BitmapHash64 bitmapHash64, C context) {
        return visitScalarFunction(bitmapHash64, context);
    }

    default R visitBitmapMax(BitmapMax bitmapMax, C context) {
        return visitScalarFunction(bitmapMax, context);
    }

    default R visitBitmapMin(BitmapMin bitmapMin, C context) {
        return visitScalarFunction(bitmapMin, context);
    }

    default R visitBitmapNot(BitmapNot bitmapNot, C context) {
        return visitScalarFunction(bitmapNot, context);
    }

    default R visitBitmapOr(BitmapOr bitmapOr, C context) {
        return visitScalarFunction(bitmapOr, context);
    }

    default R visitBitmapOrCount(BitmapOrCount bitmapOrCount, C context) {
        return visitScalarFunction(bitmapOrCount, context);
    }

    default R visitBitmapSubsetInRange(BitmapSubsetInRange bitmapSubsetInRange, C context) {
        return visitScalarFunction(bitmapSubsetInRange, context);
    }

    default R visitBitmapSubsetLimit(BitmapSubsetLimit bitmapSubsetLimit, C context) {
        return visitScalarFunction(bitmapSubsetLimit, context);
    }

    default R visitBitmapToString(BitmapToString bitmapToString, C context) {
        return visitScalarFunction(bitmapToString, context);
    }

    default R visitBitmapXor(BitmapXor bitmapXor, C context) {
        return visitScalarFunction(bitmapXor, context);
    }

    default R visitBitmapXorCount(BitmapXorCount bitmapXorCount, C context) {
        return visitScalarFunction(bitmapXorCount, context);
    }

    default R visitCbrt(Cbrt cbrt, C context) {
        return visitScalarFunction(cbrt, context);
    }

    default R visitCeil(Ceil ceil, C context) {
        return visitScalarFunction(ceil, context);
    }

    default R visitCeiling(Ceiling ceiling, C context) {
        return visitScalarFunction(ceiling, context);
    }

    default R visitCharLength(CharLength charLength, C context) {
        return visitScalarFunction(charLength, context);
    }

    default R visitCharacterLength(CharacterLength characterLength, C context) {
        return visitScalarFunction(characterLength, context);
    }

    default R visitCoalesce(Coalesce coalesce, C context) {
        return visitScalarFunction(coalesce, context);
    }

    default R visitConcat(Concat concat, C context) {
        return visitScalarFunction(concat, context);
    }

    default R visitConcatWs(ConcatWs concatWs, C context) {
        return visitScalarFunction(concatWs, context);
    }

    default R visitConv(Conv conv, C context) {
        return visitScalarFunction(conv, context);
    }

    default R visitConvertTz(ConvertTz convertTz, C context) {
        return visitScalarFunction(convertTz, context);
    }

    default R visitCos(Cos cos, C context) {
        return visitScalarFunction(cos, context);
    }

    default R visitCurrentDate(CurrentDate currentDate, C context) {
        return visitScalarFunction(currentDate, context);
    }

    default R visitCurrentTime(CurrentTime currentTime, C context) {
        return visitScalarFunction(currentTime, context);
    }

    default R visitCurrentTimestamp(CurrentTimestamp currentTimestamp, C context) {
        return visitScalarFunction(currentTimestamp, context);
    }

    default R visitCurtime(Curtime curtime, C context) {
        return visitScalarFunction(curtime, context);
    }

    default R visitDate(Date date, C context) {
        return visitScalarFunction(date, context);
    }

    default R visitDateDiff(DateDiff dateDiff, C context) {
        return visitScalarFunction(dateDiff, context);
    }

    default R visitDateFormat(DateFormat dateFormat, C context) {
        return visitScalarFunction(dateFormat, context);
    }

    default R visitDateTrunc(DateTrunc dateTrunc, C context) {
        return visitScalarFunction(dateTrunc, context);
    }

    default R visitDateV2(DateV2 dateV2, C context) {
        return visitScalarFunction(dateV2, context);
    }

    default R visitDay(Day day, C context) {
        return visitScalarFunction(day, context);
    }

    default R visitDayCeil(DayCeil dayCeil, C context) {
        return visitScalarFunction(dayCeil, context);
    }

    default R visitDayFloor(DayFloor dayFloor, C context) {
        return visitScalarFunction(dayFloor, context);
    }

    default R visitDayName(DayName dayName, C context) {
        return visitScalarFunction(dayName, context);
    }

    default R visitDayOfMonth(DayOfMonth dayOfMonth, C context) {
        return visitScalarFunction(dayOfMonth, context);
    }

    default R visitDayOfWeek(DayOfWeek dayOfWeek, C context) {
        return visitScalarFunction(dayOfWeek, context);
    }

    default R visitDayOfYear(DayOfYear dayOfYear, C context) {
        return visitScalarFunction(dayOfYear, context);
    }

    default R visitDaysDiff(DaysDiff daysDiff, C context) {
        return visitScalarFunction(daysDiff, context);
    }

    default R visitDceil(Dceil dceil, C context) {
        return visitScalarFunction(dceil, context);
    }

    default R visitDegrees(Degrees degrees, C context) {
        return visitScalarFunction(degrees, context);
    }

    default R visitDexp(Dexp dexp, C context) {
        return visitScalarFunction(dexp, context);
    }

    default R visitDfloor(Dfloor dfloor, C context) {
        return visitScalarFunction(dfloor, context);
    }

    default R visitDlog1(Dlog1 dlog1, C context) {
        return visitScalarFunction(dlog1, context);
    }

    default R visitDlog10(Dlog10 dlog10, C context) {
        return visitScalarFunction(dlog10, context);
    }

    default R visitDpow(Dpow dpow, C context) {
        return visitScalarFunction(dpow, context);
    }

    default R visitDround(Dround dround, C context) {
        return visitScalarFunction(dround, context);
    }

    default R visitDsqrt(Dsqrt dsqrt, C context) {
        return visitScalarFunction(dsqrt, context);
    }

    default R visitE(E e, C context) {
        return visitScalarFunction(e, context);
    }

    default R visitElt(Elt elt, C context) {
        return visitScalarFunction(elt, context);
    }

    default R visitEndsWith(EndsWith endsWith, C context) {
        return visitScalarFunction(endsWith, context);
    }

    default R visitEsQuery(EsQuery esQuery, C context) {
        return visitScalarFunction(esQuery, context);
    }

    default R visitExp(Exp exp, C context) {
        return visitScalarFunction(exp, context);
    }

    default R visitExtractUrlParameter(ExtractUrlParameter extractUrlParameter, C context) {
        return visitScalarFunction(extractUrlParameter, context);
    }

    default R visitFindInSet(FindInSet findInSet, C context) {
        return visitScalarFunction(findInSet, context);
    }

    default R visitFloor(Floor floor, C context) {
        return visitScalarFunction(floor, context);
    }

    default R visitFmod(Fmod fmod, C context) {
        return visitScalarFunction(fmod, context);
    }

    default R visitFpow(Fpow fpow, C context) {
        return visitScalarFunction(fpow, context);
    }

    default R visitFromBase64(FromBase64 fromBase64, C context) {
        return visitScalarFunction(fromBase64, context);
    }

    default R visitFromDays(FromDays fromDays, C context) {
        return visitScalarFunction(fromDays, context);
    }

    default R visitFromUnixtime(FromUnixtime fromUnixtime, C context) {
        return visitScalarFunction(fromUnixtime, context);
    }

    default R visitGetJsonDouble(GetJsonDouble getJsonDouble, C context) {
        return visitScalarFunction(getJsonDouble, context);
    }

    default R visitGetJsonInt(GetJsonInt getJsonInt, C context) {
        return visitScalarFunction(getJsonInt, context);
    }

    default R visitGetJsonString(GetJsonString getJsonString, C context) {
        return visitScalarFunction(getJsonString, context);
    }

    default R visitGreatest(Greatest greatest, C context) {
        return visitScalarFunction(greatest, context);
    }

    default R visitHex(Hex hex, C context) {
        return visitScalarFunction(hex, context);
    }

    default R visitHllCardinality(HllCardinality hllCardinality, C context) {
        return visitScalarFunction(hllCardinality, context);
    }

    default R visitHllEmpty(HllEmpty hllEmpty, C context) {
        return visitScalarFunction(hllEmpty, context);
    }

    default R visitHllHash(HllHash hllHash, C context) {
        return visitScalarFunction(hllHash, context);
    }

    default R visitHour(Hour hour, C context) {
        return visitScalarFunction(hour, context);
    }

    default R visitHourCeil(HourCeil hourCeil, C context) {
        return visitScalarFunction(hourCeil, context);
    }

    default R visitHourFloor(HourFloor hourFloor, C context) {
        return visitScalarFunction(hourFloor, context);
    }

    default R visitHoursDiff(HoursDiff hoursDiff, C context) {
        return visitScalarFunction(hoursDiff, context);
    }

    default R visitIf(If function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitInitcap(Initcap initcap, C context) {
        return visitScalarFunction(initcap, context);
    }

    default R visitInstr(Instr instr, C context) {
        return visitScalarFunction(instr, context);
    }

    default R visitJsonArray(JsonArray jsonArray, C context) {
        return visitScalarFunction(jsonArray, context);
    }

    default R visitJsonObject(JsonObject jsonObject, C context) {
        return visitScalarFunction(jsonObject, context);
    }

    default R visitJsonQuote(JsonQuote jsonQuote, C context) {
        return visitScalarFunction(jsonQuote, context);
    }

    default R visitJsonbExistsPath(JsonbExistsPath jsonbExistsPath, C context) {
        return visitScalarFunction(jsonbExistsPath, context);
    }

    default R visitJsonbExtract(JsonbExtract jsonbExtract, C context) {
        return visitScalarFunction(jsonbExtract, context);
    }

    default R visitJsonbExtractBigint(JsonbExtractBigint jsonbExtractBigint, C context) {
        return visitScalarFunction(jsonbExtractBigint, context);
    }

    default R visitJsonbExtractBool(JsonbExtractBool jsonbExtractBool, C context) {
        return visitScalarFunction(jsonbExtractBool, context);
    }

    default R visitJsonbExtractDouble(JsonbExtractDouble jsonbExtractDouble, C context) {
        return visitScalarFunction(jsonbExtractDouble, context);
    }

    default R visitJsonbExtractInt(JsonbExtractInt jsonbExtractInt, C context) {
        return visitScalarFunction(jsonbExtractInt, context);
    }

    default R visitJsonbExtractIsnull(JsonbExtractIsnull jsonbExtractIsnull, C context) {
        return visitScalarFunction(jsonbExtractIsnull, context);
    }

    default R visitJsonbExtractString(JsonbExtractString jsonbExtractString, C context) {
        return visitScalarFunction(jsonbExtractString, context);
    }

    default R visitJsonbParse(JsonbParse jsonbParse, C context) {
        return visitScalarFunction(jsonbParse, context);
    }

    default R visitJsonbParseErrorToInvalid(JsonbParseErrorToInvalid function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseErrorToNull(JsonbParseErrorToNull function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseErrorToValue(JsonbParseErrorToValue function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseNotnull(JsonbParseNotnull jsonbParseNotnull, C context) {
        return visitScalarFunction(jsonbParseNotnull, context);
    }

    default R visitJsonbParseNotnullErrorToInvalid(JsonbParseNotnullErrorToInvalid function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseNotnullErrorToValue(JsonbParseNotnullErrorToValue function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseNullable(JsonbParseNullable jsonbParseNullable, C context) {
        return visitScalarFunction(jsonbParseNullable, context);
    }

    default R visitJsonbParseNullableErrorToInvalid(JsonbParseNullableErrorToInvalid function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseNullableErrorToNull(JsonbParseNullableErrorToNull function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbParseNullableErrorToValue(JsonbParseNullableErrorToValue function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitJsonbType(JsonbType jsonbType, C context) {
        return visitScalarFunction(jsonbType, context);
    }

    default R visitLeast(Least least, C context) {
        return visitScalarFunction(least, context);
    }

    default R visitLeft(Left left, C context) {
        return visitScalarFunction(left, context);
    }

    default R visitLength(Length length, C context) {
        return visitScalarFunction(length, context);
    }

    default R visitLn(Ln ln, C context) {
        return visitScalarFunction(ln, context);
    }

    default R visitLocalTime(LocalTime localTime, C context) {
        return visitScalarFunction(localTime, context);
    }

    default R visitLocalTimestamp(LocalTimestamp localTimestamp, C context) {
        return visitScalarFunction(localTimestamp, context);
    }

    default R visitLocate(Locate locate, C context) {
        return visitScalarFunction(locate, context);
    }

    default R visitLog(Log log, C context) {
        return visitScalarFunction(log, context);
    }

    default R visitLog10(Log10 log10, C context) {
        return visitScalarFunction(log10, context);
    }

    default R visitLog2(Log2 log2, C context) {
        return visitScalarFunction(log2, context);
    }

    default R visitLower(Lower lower, C context) {
        return visitScalarFunction(lower, context);
    }

    default R visitLpad(Lpad lpad, C context) {
        return visitScalarFunction(lpad, context);
    }

    default R visitLtrim(Ltrim ltrim, C context) {
        return visitScalarFunction(ltrim, context);
    }

    default R visitMakeDate(MakeDate makeDate, C context) {
        return visitScalarFunction(makeDate, context);
    }

    default R visitMd5(Md5 md5, C context) {
        return visitScalarFunction(md5, context);
    }

    default R visitMd5Sum(Md5Sum md5Sum, C context) {
        return visitScalarFunction(md5Sum, context);
    }

    default R visitMinute(Minute minute, C context) {
        return visitScalarFunction(minute, context);
    }

    default R visitMinuteCeil(MinuteCeil minuteCeil, C context) {
        return visitScalarFunction(minuteCeil, context);
    }

    default R visitMinuteFloor(MinuteFloor minuteFloor, C context) {
        return visitScalarFunction(minuteFloor, context);
    }

    default R visitMinutesDiff(MinutesDiff minutesDiff, C context) {
        return visitScalarFunction(minutesDiff, context);
    }

    default R visitMoneyFormat(MoneyFormat moneyFormat, C context) {
        return visitScalarFunction(moneyFormat, context);
    }

    default R visitMonth(Month month, C context) {
        return visitScalarFunction(month, context);
    }

    default R visitMonthCeil(MonthCeil monthCeil, C context) {
        return visitScalarFunction(monthCeil, context);
    }

    default R visitMonthFloor(MonthFloor monthFloor, C context) {
        return visitScalarFunction(monthFloor, context);
    }

    default R visitMonthName(MonthName monthName, C context) {
        return visitScalarFunction(monthName, context);
    }

    default R visitMonthsDiff(MonthsDiff monthsDiff, C context) {
        return visitScalarFunction(monthsDiff, context);
    }

    default R visitMurmurHash332(MurmurHash332 murmurHash332, C context) {
        return visitScalarFunction(murmurHash332, context);
    }

    default R visitMurmurHash364(MurmurHash364 murmurHash364, C context) {
        return visitScalarFunction(murmurHash364, context);
    }

    default R visitNegative(Negative negative, C context) {
        return visitScalarFunction(negative, context);
    }

    default R visitNotNullOrEmpty(NotNullOrEmpty notNullOrEmpty, C context) {
        return visitScalarFunction(notNullOrEmpty, context);
    }

    default R visitNow(Now now, C context) {
        return visitScalarFunction(now, context);
    }

    default R visitNullIf(NullIf nullIf, C context) {
        return visitScalarFunction(nullIf, context);
    }

    default R visitNullOrEmpty(NullOrEmpty nullOrEmpty, C context) {
        return visitScalarFunction(nullOrEmpty, context);
    }

    default R visitNvl(Nvl nvl, C context) {
        return visitScalarFunction(nvl, context);
    }

    default R visitParseUrl(ParseUrl parseUrl, C context) {
        return visitScalarFunction(parseUrl, context);
    }

    default R visitPi(Pi pi, C context) {
        return visitScalarFunction(pi, context);
    }

    default R visitPmod(Pmod pmod, C context) {
        return visitScalarFunction(pmod, context);
    }

    default R visitPositive(Positive positive, C context) {
        return visitScalarFunction(positive, context);
    }

    default R visitPow(Pow pow, C context) {
        return visitScalarFunction(pow, context);
    }

    default R visitPower(Power power, C context) {
        return visitScalarFunction(power, context);
    }

    default R visitQuantilePercent(QuantilePercent quantilePercent, C context) {
        return visitScalarFunction(quantilePercent, context);
    }

    default R visitQuarter(Quarter quarter, C context) {
        return visitScalarFunction(quarter, context);
    }

    default R visitRadians(Radians radians, C context) {
        return visitScalarFunction(radians, context);
    }

    default R visitRandom(Random random, C context) {
        return visitScalarFunction(random, context);
    }

    default R visitRegexpExtract(RegexpExtract regexpExtract, C context) {
        return visitScalarFunction(regexpExtract, context);
    }

    default R visitRegexpReplace(RegexpReplace regexpReplace, C context) {
        return visitScalarFunction(regexpReplace, context);
    }

    default R visitRepeat(Repeat repeat, C context) {
        return visitScalarFunction(repeat, context);
    }

    default R visitReplace(Replace replace, C context) {
        return visitScalarFunction(replace, context);
    }

    default R visitReverse(Reverse reverse, C context) {
        return visitScalarFunction(reverse, context);
    }

    default R visitRight(Right right, C context) {
        return visitScalarFunction(right, context);
    }

    default R visitRound(Round round, C context) {
        return visitScalarFunction(round, context);
    }

    default R visitRpad(Rpad rpad, C context) {
        return visitScalarFunction(rpad, context);
    }

    default R visitRtrim(Rtrim rtrim, C context) {
        return visitScalarFunction(rtrim, context);
    }

    default R visitSecond(Second second, C context) {
        return visitScalarFunction(second, context);
    }

    default R visitSecondCeil(SecondCeil secondCeil, C context) {
        return visitScalarFunction(secondCeil, context);
    }

    default R visitSecondFloor(SecondFloor secondFloor, C context) {
        return visitScalarFunction(secondFloor, context);
    }

    default R visitSecondsDiff(SecondsDiff secondsDiff, C context) {
        return visitScalarFunction(secondsDiff, context);
    }

    default R visitSign(Sign sign, C context) {
        return visitScalarFunction(sign, context);
    }

    default R visitSin(Sin sin, C context) {
        return visitScalarFunction(sin, context);
    }

    default R visitSleep(Sleep sleep, C context) {
        return visitScalarFunction(sleep, context);
    }

    default R visitSm3(Sm3 sm3, C context) {
        return visitScalarFunction(sm3, context);
    }

    default R visitSm3sum(Sm3sum sm3sum, C context) {
        return visitScalarFunction(sm3sum, context);
    }

    default R visitSm4Decrypt(Sm4Decrypt sm4Decrypt, C context) {
        return visitScalarFunction(sm4Decrypt, context);
    }

    default R visitSm4Encrypt(Sm4Encrypt sm4Encrypt, C context) {
        return visitScalarFunction(sm4Encrypt, context);
    }

    default R visitSpace(Space space, C context) {
        return visitScalarFunction(space, context);
    }

    default R visitSplitPart(SplitPart splitPart, C context) {
        return visitScalarFunction(splitPart, context);
    }

    default R visitSqrt(Sqrt sqrt, C context) {
        return visitScalarFunction(sqrt, context);
    }

    default R visitStAstext(StAstext stAstext, C context) {
        return visitScalarFunction(stAstext, context);
    }

    default R visitStAswkt(StAswkt stAswkt, C context) {
        return visitScalarFunction(stAswkt, context);
    }

    default R visitStCircle(StCircle stCircle, C context) {
        return visitScalarFunction(stCircle, context);
    }

    default R visitStContains(StContains stContains, C context) {
        return visitScalarFunction(stContains, context);
    }

    default R visitStDistanceSphere(StDistanceSphere stDistanceSphere, C context) {
        return visitScalarFunction(stDistanceSphere, context);
    }

    default R visitStGeometryfromtext(StGeometryfromtext stGeometryfromtext, C context) {
        return visitScalarFunction(stGeometryfromtext, context);
    }

    default R visitStGeomfromtext(StGeomfromtext stGeomfromtext, C context) {
        return visitScalarFunction(stGeomfromtext, context);
    }

    default R visitStLinefromtext(StLinefromtext stLinefromtext, C context) {
        return visitScalarFunction(stLinefromtext, context);
    }

    default R visitStLinestringfromtext(StLinestringfromtext stLinestringfromtext, C context) {
        return visitScalarFunction(stLinestringfromtext, context);
    }

    default R visitStPoint(StPoint stPoint, C context) {
        return visitScalarFunction(stPoint, context);
    }

    default R visitStPolyfromtext(StPolyfromtext stPolyfromtext, C context) {
        return visitScalarFunction(stPolyfromtext, context);
    }

    default R visitStPolygon(StPolygon stPolygon, C context) {
        return visitScalarFunction(stPolygon, context);
    }

    default R visitStPolygonfromtext(StPolygonfromtext stPolygonfromtext, C context) {
        return visitScalarFunction(stPolygonfromtext, context);
    }

    default R visitStX(StX stX, C context) {
        return visitScalarFunction(stX, context);
    }

    default R visitStY(StY stY, C context) {
        return visitScalarFunction(stY, context);
    }

    default R visitStartsWith(StartsWith startsWith, C context) {
        return visitScalarFunction(startsWith, context);
    }

    default R visitStrLeft(StrLeft strLeft, C context) {
        return visitScalarFunction(strLeft, context);
    }

    default R visitStrRight(StrRight strRight, C context) {
        return visitScalarFunction(strRight, context);
    }

    default R visitStrToDate(StrToDate strToDate, C context) {
        return visitScalarFunction(strToDate, context);
    }

    default R visitSubBitmap(SubBitmap subBitmap, C context) {
        return visitScalarFunction(subBitmap, context);
    }

    default R visitSubstring(Substring substring, C context) {
        return visitScalarFunction(substring, context);
    }

    default R visitTan(Tan tan, C context) {
        return visitScalarFunction(tan, context);
    }

    default R visitTimeDiff(TimeDiff timeDiff, C context) {
        return visitScalarFunction(timeDiff, context);
    }

    default R visitTimestamp(Timestamp timestamp, C context) {
        return visitScalarFunction(timestamp, context);
    }

    default R visitToBase64(ToBase64 toBase64, C context) {
        return visitScalarFunction(toBase64, context);
    }

    default R visitToBitmap(ToBitmap toBitmap, C context) {
        return visitScalarFunction(toBitmap, context);
    }

    default R visitToBitmapWithCheck(ToBitmapWithCheck toBitmapWithCheck, C context) {
        return visitScalarFunction(toBitmapWithCheck, context);
    }

    default R visitToDate(ToDate toDate, C context) {
        return visitScalarFunction(toDate, context);
    }

    default R visitToDateV2(ToDateV2 toDateV2, C context) {
        return visitScalarFunction(toDateV2, context);
    }

    default R visitToDays(ToDays toDays, C context) {
        return visitScalarFunction(toDays, context);
    }

    default R visitToQuantileState(ToQuantileState toQuantileState, C context) {
        return visitScalarFunction(toQuantileState, context);
    }

    default R visitTrim(Trim trim, C context) {
        return visitScalarFunction(trim, context);
    }

    default R visitTruncate(Truncate truncate, C context) {
        return visitScalarFunction(truncate, context);
    }

    default R visitUnhex(Unhex unhex, C context) {
        return visitScalarFunction(unhex, context);
    }

    default R visitUnixTimestamp(UnixTimestamp unixTimestamp, C context) {
        return visitScalarFunction(unixTimestamp, context);
    }

    default R visitUpper(Upper upper, C context) {
        return visitScalarFunction(upper, context);
    }

    default R visitUtcTimestamp(UtcTimestamp utcTimestamp, C context) {
        return visitScalarFunction(utcTimestamp, context);
    }

    default R visitVersion(Version version, C context) {
        return visitScalarFunction(version, context);
    }

    default R visitWeek(Week week, C context) {
        return visitScalarFunction(week, context);
    }

    default R visitWeekCeil(WeekCeil weekCeil, C context) {
        return visitScalarFunction(weekCeil, context);
    }

    default R visitWeekFloor(WeekFloor weekFloor, C context) {
        return visitScalarFunction(weekFloor, context);
    }

    default R visitWeekOfYear(WeekOfYear weekOfYear, C context) {
        return visitScalarFunction(weekOfYear, context);
    }

    default R visitWeekday(Weekday weekday, C context) {
        return visitScalarFunction(weekday, context);
    }

    default R visitWeeksDiff(WeeksDiff weeksDiff, C context) {
        return visitScalarFunction(weeksDiff, context);
    }

    default R visitYear(Year year, C context) {
        return visitScalarFunction(year, context);
    }

    default R visitYearCeil(YearCeil yearCeil, C context) {
        return visitScalarFunction(yearCeil, context);
    }

    default R visitYearFloor(YearFloor yearFloor, C context) {
        return visitScalarFunction(yearFloor, context);
    }

    default R visitYearWeek(YearWeek yearWeek, C context) {
        return visitScalarFunction(yearWeek, context);
    }

    default R visitYearsDiff(YearsDiff yearsDiff, C context) {
        return visitScalarFunction(yearsDiff, context);
    }
}
