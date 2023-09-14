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

import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Regexp;
import org.apache.doris.nereids.trees.expressions.StringRegexPredicate;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.*;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;

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

    default R visitAesDecryptV2(AesDecryptV2 aesDecryptV2, C context) {
        return visitScalarFunction(aesDecryptV2, context);
    }

    default R visitAesEncrypt(AesEncrypt aesEncrypt, C context) {
        return visitScalarFunction(aesEncrypt, context);
    }

    default R visitAesEncryptV2(AesEncryptV2 aesEncryptV2, C context) {
        return visitScalarFunction(aesEncryptV2, context);
    }

    default R visitAppendTrailingCharIfAbsent(AppendTrailingCharIfAbsent function, C context) {
        return visitScalarFunction(function, context);
    }

    default R visitArray(Array array, C context) {
        return visitScalarFunction(array, context);
    }

    default R visitArrayAvg(ArrayAvg arrayAvg, C context) {
        return visitScalarFunction(arrayAvg, context);
    }

    default R visitArrayCompact(ArrayCompact arrayCompact, C context) {
        return visitScalarFunction(arrayCompact, context);
    }

    default R visitArrayContains(ArrayContains arrayContains, C context) {
        return visitScalarFunction(arrayContains, context);
    }

    default R visitArrayCumSum(ArrayCumSum arrayCumSum, C context) {
        return visitScalarFunction(arrayCumSum, context);
    }

    default R visitArrayDifference(ArrayDifference arrayDifference, C context) {
        return visitScalarFunction(arrayDifference, context);
    }

    default R visitArrayDistinct(ArrayDistinct arrayDistinct, C context) {
        return visitScalarFunction(arrayDistinct, context);
    }

    default R visitArrayEnumerate(ArrayEnumerate arrayEnumerate, C context) {
        return visitScalarFunction(arrayEnumerate, context);
    }

    default R visitArrayExcept(ArrayExcept arrayExcept, C context) {
        return visitScalarFunction(arrayExcept, context);
    }

    default R visitArrayIntersect(ArrayIntersect arrayIntersect, C context) {
        return visitScalarFunction(arrayIntersect, context);
    }

    default R visitArrayJoin(ArrayJoin arrayJoin, C context) {
        return visitScalarFunction(arrayJoin, context);
    }

    default R visitArrayMax(ArrayMax arrayMax, C context) {
        return visitScalarFunction(arrayMax, context);
    }

    default R visitArrayMin(ArrayMin arrayMin, C context) {
        return visitScalarFunction(arrayMin, context);
    }

    default R visitArrayPopBack(ArrayPopBack arrayPopBack, C context) {
        return visitScalarFunction(arrayPopBack, context);
    }

    default R visitArrayPopFront(ArrayPopFront arrayPopFront, C context) {
        return visitScalarFunction(arrayPopFront, context);
    }

    default R visitArrayPosition(ArrayPosition arrayPosition, C context) {
        return visitScalarFunction(arrayPosition, context);
    }

    default R visitArrayProduct(ArrayProduct arrayProduct, C context) {
        return visitScalarFunction(arrayProduct, context);
    }

    default R visitArrayRange(ArrayRange arrayRange, C context) {
        return visitScalarFunction(arrayRange, context);
    }

    default R visitArrayRemove(ArrayRemove arrayRemove, C context) {
        return visitScalarFunction(arrayRemove, context);
    }

    default R visitArraySlice(ArraySlice arraySlice, C context) {
        return visitScalarFunction(arraySlice, context);
    }

    default R visitArraySort(ArraySort arraySort, C context) {
        return visitScalarFunction(arraySort, context);
    }

    default R visitArrayMap(ArrayMap arraySort, C context) {
        return visitScalarFunction(arraySort, context);
    }

    default R visitArraySum(ArraySum arraySum, C context) {
        return visitScalarFunction(arraySum, context);
    }

    default R visitArrayUnion(ArrayUnion arrayUnion, C context) {
        return visitScalarFunction(arrayUnion, context);
    }

    default R visitArrayWithConstant(ArrayWithConstant arrayWithConstant, C context) {
        return visitScalarFunction(arrayWithConstant, context);
    }

    default R visitArraysOverlap(ArraysOverlap arraysOverlap, C context) {
        return visitScalarFunction(arraysOverlap, context);
    }

    default R visitArrayReverseSort(ArrayReverseSort arrayReverseSort, C context) {
        return visitScalarFunction(arrayReverseSort, context);
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

    default R visitBitmapFromArray(BitmapFromArray bitmapFromArray, C context) {
        return visitScalarFunction(bitmapFromArray, context);
    }

    default R visitBitmapFromString(BitmapFromString bitmapFromString, C context) {
        return visitScalarFunction(bitmapFromString, context);
    }

    default R visitBitmapFromBase64(BitmapFromBase64 bitmapFromBase64, C context) {
        return visitScalarFunction(bitmapFromBase64, context);
    }

    default R visitBitmapToBase64(BitmapToBase64 bitmapToBase64, C context) {
        return visitScalarFunction(bitmapToBase64, context);
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

    default R visitBitmapRemove(BitmapRemove bitmapRemove, C context) {
        return visitScalarFunction(bitmapRemove, context);
    }

    default R visitBitmapSubsetInRange(BitmapSubsetInRange bitmapSubsetInRange, C context) {
        return visitScalarFunction(bitmapSubsetInRange, context);
    }

    default R visitBitmapSubsetLimit(BitmapSubsetLimit bitmapSubsetLimit, C context) {
        return visitScalarFunction(bitmapSubsetLimit, context);
    }

    default R visitBitmapToArray(BitmapToArray bitmapToArray, C context) {
        return visitScalarFunction(bitmapToArray, context);
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

    default R visitCardinality(Cardinality cardinality, C context) {
        return visitScalarFunction(cardinality, context);
    }

    default R visitCbrt(Cbrt cbrt, C context) {
        return visitScalarFunction(cbrt, context);
    }

    default R visitCeil(Ceil ceil, C context) {
        return visitScalarFunction(ceil, context);
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

    default R visitChar(Char charFunc, C context) {
        return visitScalarFunction(charFunc, context);
    }

    default R visitConcatWs(ConcatWs concatWs, C context) {
        return visitScalarFunction(concatWs, context);
    }

    default R visitConv(Conv conv, C context) {
        return visitScalarFunction(conv, context);
    }

    default R visitConvertTo(ConvertTo convertTo, C context) {
        return visitScalarFunction(convertTo, context);
    }

    default R visitConvertTz(ConvertTz convertTz, C context) {
        return visitScalarFunction(convertTz, context);
    }

    default R visitCos(Cos cos, C context) {
        return visitScalarFunction(cos, context);
    }

    default R visitCountEqual(CountEqual countequal, C context) {
        return visitScalarFunction(countequal, context);
    }

    default R visitCurrentDate(CurrentDate currentDate, C context) {
        return visitScalarFunction(currentDate, context);
    }

    default R visitCurrentTime(CurrentTime currentTime, C context) {
        return visitScalarFunction(currentTime, context);
    }

    default R visitDate(Date date, C context) {
        return visitScalarFunction(date, context);
    }

    default R visitDatabase(Database database, C context) {
        return visitScalarFunction(database, context);
    }

    default R visitCurrentUser(CurrentUser currentUser, C context) {
        return visitScalarFunction(currentUser, context);
    }

    default R visitCurrentCatalog(CurrentCatalog currentCatalog, C context) {
        return visitScalarFunction(currentCatalog, context);
    }

    default R visitUser(User user, C context) {
        return visitScalarFunction(user, context);
    }

    default R visitConnectionId(ConnectionId connectionId, C context) {
        return visitScalarFunction(connectionId, context);
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

    default R visitDaysAdd(DaysAdd daysAdd, C context) {
        return visitScalarFunction(daysAdd, context);
    }

    default R visitDaysSub(DaysSub daysSub, C context) {
        return visitScalarFunction(daysSub, context);
    }

    default R visitDigitalMasking(DigitalMasking digitalMasking, C context) {
        return visitScalarFunction(digitalMasking, context);
    }

    default R visitYearsSub(YearsSub yearsSub, C context) {
        return visitScalarFunction(yearsSub, context);
    }

    default R visitMonthsSub(MonthsSub monthsSub, C context) {
        return visitScalarFunction(monthsSub, context);
    }

    default R visitHoursSub(HoursSub hoursSub, C context) {
        return visitScalarFunction(hoursSub, context);
    }

    default R visitMinutesSub(MinutesSub minutesSub, C context) {
        return visitScalarFunction(minutesSub, context);
    }

    default R visitSecondsSub(SecondsSub secondsSub, C context) {
        return visitScalarFunction(secondsSub, context);
    }

    default R visitMonthsAdd(MonthsAdd monthsAdd, C context) {
        return visitScalarFunction(monthsAdd, context);
    }

    default R visitYearsAdd(YearsAdd yearsAdd, C context) {
        return visitScalarFunction(yearsAdd, context);
    }

    default R visitHoursAdd(HoursAdd hoursAdd, C context) {
        return visitScalarFunction(hoursAdd, context);
    }

    default R visitMinutesAdd(MinutesAdd minutesAdd, C context) {
        return visitScalarFunction(minutesAdd, context);
    }

    default R visitSecondsAdd(SecondsAdd secondsAdd, C context) {
        return visitScalarFunction(secondsAdd, context);
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

    default R visitDomain(Domain domain, C context) {
        return visitScalarFunction(domain, context);
    }

    default R visitDomainWithoutWww(DomainWithoutWww domainWithoutWww, C context) {
        return visitScalarFunction(domainWithoutWww, context);
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

    default R visitElementAt(ElementAt elementAt, C context) {
        return visitScalarFunction(elementAt, context);
    }

    default R visitElt(Elt elt, C context) {
        return visitScalarFunction(elt, context);
    }

    default R visitEndsWith(EndsWith endsWith, C context) {
        return visitScalarFunction(endsWith, context);
    }

    default R visitEncryptKeyRef(EncryptKeyRef encryptKeyRef, C context) {
        return visitScalarFunction(encryptKeyRef, context);
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

    default R visitField(Field field, C context) {
        return visitScalarFunction(field, context);
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

    default R visitGetJsonBigInt(GetJsonBigInt getJsonBigInt, C context) {
        return visitScalarFunction(getJsonBigInt, context);
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

    default R visitJsonExtract(JsonExtract jsonExtract, C context) {
        return visitScalarFunction(jsonExtract, context);
    }

    default R visitJsonQuote(JsonQuote jsonQuote, C context) {
        return visitScalarFunction(jsonQuote, context);
    }

    default R visitJsonUnQuote(JsonUnQuote jsonUnQuote, C context) {
        return visitScalarFunction(jsonUnQuote, context);
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

    default R visitJsonbExtractLargeint(JsonbExtractLargeint jsonbExtractLargeint, C context) {
        return visitScalarFunction(jsonbExtractLargeint, context);
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

    default R visitLastDay(LastDay lastDay, C context) {
        return visitScalarFunction(lastDay, context);
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

    default R visitLike(Like like, C context) {
        return visitStringRegexPredicate(like, context);
    }

    default R visitLn(Ln ln, C context) {
        return visitScalarFunction(ln, context);
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

    default R visitMask(Mask mask, C context) {
        return visitScalarFunction(mask, context);
    }

    default R visitMaskFirstN(MaskFirstN maskFirstN, C context) {
        return visitScalarFunction(maskFirstN, context);
    }

    default R visitMaskLastN(MaskLastN maskLastN, C context) {
        return visitScalarFunction(maskLastN, context);
    }

    default R visitMd5(Md5 md5, C context) {
        return visitScalarFunction(md5, context);
    }

    default R visitMd5Sum(Md5Sum md5Sum, C context) {
        return visitScalarFunction(md5Sum, context);
    }

    default R visitMicrosecond(Microsecond microsecond, C context) {
        return visitScalarFunction(microsecond, context);
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

    default R visitMultiMatchAny(MultiMatchAny multiMatchAny, C context) {
        return visitScalarFunction(multiMatchAny, context);
    }

    default R visitMultiSearchAllPositions(MultiSearchAllPositions function, C context) {
        return visitScalarFunction(function, context);
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

    default R visitPassword(Password password, C context) {
        return visitScalarFunction(password, context);
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

    default R visitProtocol(Protocol protocol, C context) {
        return visitScalarFunction(protocol, context);
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

    default R visitRegexp(Regexp regexp, C context) {
        return visitStringRegexPredicate(regexp, context);
    }

    default R visitRegexpExtract(RegexpExtract regexpExtract, C context) {
        return visitScalarFunction(regexpExtract, context);
    }

    default R visitRegexpExtractAll(RegexpExtractAll regexpExtractAll, C context) {
        return visitScalarFunction(regexpExtractAll, context);
    }

    default R visitRegexpReplace(RegexpReplace regexpReplace, C context) {
        return visitScalarFunction(regexpReplace, context);
    }

    default R visitRegexpReplaceOne(RegexpReplaceOne regexpReplaceOne, C context) {
        return visitScalarFunction(regexpReplaceOne, context);
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

    default R visitRoundBankers(RoundBankers roundBankers, C context) {
        return visitScalarFunction(roundBankers, context);
    }

    default R visitRpad(Rpad rpad, C context) {
        return visitScalarFunction(rpad, context);
    }

    default R visitRtrim(Rtrim rtrim, C context) {
        return visitScalarFunction(rtrim, context);
    }

    default R visitRunningDifference(RunningDifference runningDifference, C context) {
        return visitScalarFunction(runningDifference, context);
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

    default R visitSha2(Sha2 sha2, C context) {
        return visitScalarFunction(sha2, context);
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

    default R visitSm4DecryptV2(Sm4DecryptV2 sm4DecryptV2, C context) {
        return visitScalarFunction(sm4DecryptV2, context);
    }

    default R visitSm4Encrypt(Sm4Encrypt sm4Encrypt, C context) {
        return visitScalarFunction(sm4Encrypt, context);
    }

    default R visitSm4EncryptV2(Sm4EncryptV2 sm4EncryptV2, C context) {
        return visitScalarFunction(sm4EncryptV2, context);
    }

    default R visitSpace(Space space, C context) {
        return visitScalarFunction(space, context);
    }

    default R visitSplitByChar(SplitByChar splitByChar, C context) {
        return visitScalarFunction(splitByChar, context);
    }

    default R visitSplitByString(SplitByString splitByString, C context) {
        return visitScalarFunction(splitByString, context);
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

    default R visitStAngleSphere(StAngleSphere stAngleSphere, C context) {
        return visitScalarFunction(stAngleSphere, context);
    }

    default R visitStAngle(StAngle stAngle, C context) {
        return visitScalarFunction(stAngle, context);
    }

    default R visitStAzimuth(StAzimuth stAzimuth, C context) {
        return visitScalarFunction(stAzimuth, context);
    }

    default R visitStAreaSquareMeters(StAreaSquareMeters stAreaSquareMeters, C context) {
        return visitScalarFunction(stAreaSquareMeters, context);
    }

    default R visitStAreaSquareKm(StAreaSquareKm stAreaSquareKm, C context) {
        return visitScalarFunction(stAreaSquareKm, context);
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

    default R visitStGeometryfromwkb(StGeometryFromWKB stGeometryfromwkb, C context) {
        return visitScalarFunction(stGeometryfromwkb, context);
    }

    default R visitStGeomfromwkb(StGeomFromWKB stGeomfromwkb, C context) {
        return visitScalarFunction(stGeomfromwkb, context);
    }

    default R visitStAsBinary(StAsBinary stAsBinary, C context) {
        return visitScalarFunction(stAsBinary, context);
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

    default R visitStringRegexPredicate(StringRegexPredicate stringRegexPredicate, C context) {
        return visitScalarFunction(stringRegexPredicate, context);
    }

    default R visitSubBitmap(SubBitmap subBitmap, C context) {
        return visitScalarFunction(subBitmap, context);
    }

    default R visitSubReplace(SubReplace subReplace, C context) {
        return visitScalarFunction(subReplace, context);
    }

    default R visitSubstring(Substring substring, C context) {
        return visitScalarFunction(substring, context);
    }

    default R visitSubstringIndex(SubstringIndex substringIndex, C context) {
        return visitScalarFunction(substringIndex, context);
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

    default R visitToMonday(ToMonday toMonday, C context) {
        return visitScalarFunction(toMonday, context);
    }

    default R visitTokenize(Tokenize tokenize, C context) {
        return visitScalarFunction(tokenize, context);
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

    default R visitUuid(Uuid uuid, C context) {
        return visitScalarFunction(uuid, context);
    }

    default R visitUuidNumeric(UuidNumeric uuidNumeric, C context) {
        return visitScalarFunction(uuidNumeric, context);
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

    default R visitWeeksAdd(WeeksAdd weeksAdd, C context) {
        return visitScalarFunction(weeksAdd, context);
    }

    default R visitWeeksDiff(WeeksDiff weeksDiff, C context) {
        return visitScalarFunction(weeksDiff, context);
    }

    default R visitWeeksSub(WeeksSub weeksSub, C context) {
        return visitScalarFunction(weeksSub, context);
    }

    default R visitWidthBucket(WidthBucket widthBucket, C context) {
        return visitScalarFunction(widthBucket, context);
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

    default R visitStateCombinator(StateCombinator combinator, C context) {
        return visitScalarFunction(combinator, context);
    }

    default R visitJavaUdf(JavaUdf javaUdf, C context) {
        return visitScalarFunction(javaUdf, context);
    }

    default R visitAliasUdf(AliasUdf aliasUdf, C context) {
        return visitScalarFunction(aliasUdf, context);
    }

    // map functions

    default R visitCreateMap(CreateMap createMap, C context) {
        return visitScalarFunction(createMap, context);
    }

    default R visitMapContainsKey(MapContainsKey mapContainsKey, C context) {
        return visitScalarFunction(mapContainsKey, context);
    }

    default R visitMapContainsValue(MapContainsValue mapContainsValue, C context) {
        return visitScalarFunction(mapContainsValue, context);
    }

    default R visitMapKeys(MapKeys mapKeys, C context) {
        return visitScalarFunction(mapKeys, context);
    }

    default R visitMapSize(MapSize mapSize, C context) {
        return visitScalarFunction(mapSize, context);
    }

    default R visitMapValues(MapValues mapValues, C context) {
        return visitScalarFunction(mapValues, context);
    }

    // struct function

    default R visitCreateStruct(CreateStruct createStruct, C context) {
        return visitScalarFunction(createStruct, context);
    }

    default R visitCreateNamedStruct(CreateNamedStruct createNamedStruct, C context) {
        return visitScalarFunction(createNamedStruct, context);
    }

    default R visitStructElement(StructElement structElement, C context) {
        return visitScalarFunction(structElement, context);
    }
}
