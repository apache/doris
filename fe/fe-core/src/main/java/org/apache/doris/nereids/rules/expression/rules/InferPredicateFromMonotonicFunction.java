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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.DateCeilFloorMonotonic;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.FromUnixtime;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Left;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MonthsSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.QuartersSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StrToDate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Substring;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Year;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.YearsSub;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NumericLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Infer an extra bare-column range predicate from a monotonic-function predicate. The derived
 * predicate is an ordinary conjunct, so it is consumed by any storage-layer min/max statistics --
 * external ORC/Parquet row-group pruning, internal OLAP zonemap pruning, and partition pruning
 * (the rule runs at the logical-plan level and is NOT scan-type specific). For relaxed (one-way /
 * superset) inference the original predicate is ALWAYS kept, because the derived one is only a
 * necessary condition, not an equivalent rewrite.
 *
 * <p>This covers these families of lower-bound comparisons ({@code >=}, {@code >}):
 * <ul>
 *   <li><b>String prefix</b> {@code substring(col,1,N)} / {@code left(col,N)} — relaxed superset,
 *       sound when {@code codepoints(c) <= N} under binary collation.</li>
 *   <li><b>Date floor</b> {@code date_trunc(col,'unit')} / {@code date(col)} / {@code to_date(col)}
 *       — these satisfy the floor property {@code f(col) <= col}, so
 *       {@code f(col) op c ==> col op c} holds for ANY constant {@code c}. Pure calendar
 *       arithmetic, no timezone / DST involved, so this is provably correct.</li>
 *   <li><b>from_unixtime</b> {@code from_unixtime(epochCol) op c} — maps an epoch to session-local
 *       wall clock, strictly monotonic except across UTC-offset transitions, so the bare-epoch bound
 *       {@code epochCol op unix_timestamp(c)} is emitted only when no offset transition falls in a
 *       guard band just below the constant's epoch (see {@link #tryInferFromUnixtime}). The guard is
 *       per-predicate, not per-zone: a constant far from any transition derives even in a DST zone.</li>
 *   <li><b>months/years/quarters_add</b> {@code months_add(col,k) op c} — monotone non-decreasing
 *       (end-of-month clamping only creates plateaus, never decreases), so NOT a floor; it reverses
 *       via the paired {@code _sub} to a lower bound {@code col >= months_sub(c,k)}. Only the lower
 *       bound ({@code >=}, {@code >}) is sound; upper bounds are excluded (see
 *       {@link #tryInferMonthsYearsAdd}). Pure calendar arithmetic on DateV2/DateTimeV2, no DST.</li>
 *   <li><b>year</b> {@code year(col) op Y} — the only globally monotonic date-component extractor
 *       (month/hour/day are periodic and have no bare-column preimage). Timezone-free, so
 *       {@code year(col) = Y <=> col in [Y-01-01, (Y+1)-01-01)} is an EXACT preimage; all of
 *       {@code =, >, >=, <, <=} map to a bare-column bound (see {@link #tryInferYear}).</li>
 * </ul>
 * Numeric arithmetic ({@code col+k op c}) is already covered by SimplifyArithmeticComparisonRule.
 *
 * <p>Only lower-bound comparisons are sound for a prefix:
 * <pre>
 *   substring(col, 1, N) &gt;= 'c'  ==&gt;  col &gt;= 'c'
 *   substring(col, 1, N) &gt;  'c'  ==&gt;  col &gt;  'c'
 *   left(col, N)         &gt;= 'c'  ==&gt;  col &gt;= 'c'
 *   left(col, N)         &gt;  'c'  ==&gt;  col &gt;  'c'
 * </pre>
 * Upper-bound comparisons ({@code <}, {@code <=}) have no sound bare-column bound from a prefix
 * (a longer string can exceed the prefix bound), so they are not inferred here.
 *
 * <p>Correctness relies on binary collation, under which byte-wise lexicographic order (used by
 * ORC/Parquet min/max) equals code-point order (used by Doris comparison). {@code substring}/
 * {@code left} cut on code-point boundaries, so {@code prefix(col) op c} implies {@code col op c}
 * whenever {@code len(c) <= N}. When {@code len(c) > N}, the prefix can never reach {@code c}, so
 * no sound lower bound on {@code col} can be derived and we skip the inference.
 *
 * <p>The rule matches the enclosing conjunction and appends derived predicates only when they are
 * not already present, so it is idempotent and does not grow the predicate on repeated passes.
 */
public class InferPredicateFromMonotonicFunction implements ExpressionPatternRuleFactory {

    public static final InferPredicateFromMonotonicFunction INSTANCE = new InferPredicateFromMonotonicFunction();

    // DST guard band below a from_unixtime constant's epoch: no offset transition may fall in
    // [X - band, X] for the reversed bare-epoch bound to stay sound. 2 days exceeds any real DST or
    // historical base-offset shift, so it never misses a transition that could invalidate the bound.
    private static final long DST_GUARD_BAND_SECONDS = 2 * 24 * 60 * 60;

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                // top-level AND filter: append a derived predicate per prefix comparison conjunct
                matchesTopType(CompoundPredicate.class)
                        .thenApply(ctx -> rewriteCompound((CompoundPredicate) ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.INFER_PREDICATE_FROM_MONOTONIC_FUNCTION),
                // single-predicate filter (comparison is the whole filter, no enclosing AND):
                // substring(col,1,N) op c -> and(orig, col op c). Use root() so that once wrapped in
                // the new AND the comparison is no longer the root and the rule does not re-fire.
                root(GreaterThan.class)
                        .thenApply(ctx -> rewriteSingle((ComparisonPredicate) ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.INFER_PREDICATE_FROM_MONOTONIC_FUNCTION),
                root(GreaterThanEqual.class)
                        .thenApply(ctx -> rewriteSingle((ComparisonPredicate) ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.INFER_PREDICATE_FROM_MONOTONIC_FUNCTION),
                root(LessThan.class)
                        .thenApply(ctx -> rewriteSingle((ComparisonPredicate) ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.INFER_PREDICATE_FROM_MONOTONIC_FUNCTION),
                root(LessThanEqual.class)
                        .thenApply(ctx -> rewriteSingle((ComparisonPredicate) ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.INFER_PREDICATE_FROM_MONOTONIC_FUNCTION),
                root(EqualTo.class)
                        .thenApply(ctx -> rewriteSingle((ComparisonPredicate) ctx.expr, ctx.rewriteContext))
                        .toRule(ExpressionRuleType.INFER_PREDICATE_FROM_MONOTONIC_FUNCTION)
        );
    }

    private static boolean disabled() {
        return ConnectContext.get() == null
                || !ConnectContext.get().getSessionVariable().enableInferPredicateFromMonotonicFunction;
    }

    private static Expression rewriteSingle(ComparisonPredicate cmp, ExpressionRewriteContext context) {
        if (disabled()) {
            return cmp;
        }
        Expression derived = tryInfer(cmp, context);
        if (derived == null) {
            return cmp;
        }
        // Flatten: a two-sided derived range is And(lower, upper); prepend the original and build one
        // flat conjunction (deduped) instead of a nested And(cmp, And(lower, upper)).
        List<Expression> conjuncts = Lists.newArrayList((Expression) cmp);
        conjuncts.addAll(ExpressionUtils.extractConjunction(derived));
        return ExpressionUtils.and(conjuncts);
    }

    private static Expression rewriteCompound(CompoundPredicate expr, ExpressionRewriteContext context) {
        if (disabled()) {
            return expr;
        }
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(expr);
        if (conjuncts.size() <= 1) {
            return expr;
        }
        Set<Expression> existing = Sets.newHashSet(conjuncts);
        List<Expression> derivedToAdd = Lists.newArrayList();
        for (Expression conjunct : conjuncts) {
            Expression derived = tryInfer(conjunct, context);
            if (derived == null) {
                continue;
            }
            // A two-sided derived range is And(lower, upper); flatten it and add each bound only if
            // not already present, so repeated passes stay idempotent and no nested/duplicate And forms.
            for (Expression bound : ExpressionUtils.extractConjunction(derived)) {
                if (existing.add(bound)) {
                    derivedToAdd.add(bound);
                }
            }
        }
        if (derivedToAdd.isEmpty()) {
            return expr;
        }
        List<Expression> newConjuncts = Lists.newArrayList(conjuncts);
        newConjuncts.addAll(derivedToAdd);
        return ExpressionUtils.and(newConjuncts);
    }

    // if conjunct is a comparison `f(col) op c` from which a sound bare-column predicate can be
    // derived, return that derived predicate (possibly an AND of two bounds); otherwise null.
    // Handled:
    //   1. string prefix: substring(col,1,N)/left(col,N) op strLiteral, codepoints(c) <= N
    //      - op in {>, >=}: -> col op c
    //      - op is  =     : -> col >= c AND col < succ(c)   (two-sided, prunes a tight range)
    //   2. date floor:    date_trunc(col,'unit')/date(col)/to_date(col) op c
    //      - op in {>, >=}: -> col op c            (floor property f(col) <= col)
    //      - op is  =     : -> col >= c AND col < c + 1 bucket   (preimage two-sided range)
    private static Expression tryInfer(Expression conjunct, ExpressionRewriteContext context) {
        if (!(conjunct instanceof GreaterThan) && !(conjunct instanceof GreaterThanEqual)
                && !(conjunct instanceof LessThan) && !(conjunct instanceof LessThanEqual)
                && !(conjunct instanceof EqualTo)) {
            return null;
        }
        ComparisonPredicate cmp = (ComparisonPredicate) conjunct;
        Expression left = cmp.left();
        Expression right = cmp.right();
        if (!(right instanceof Literal)) {
            return null;
        }
        Expression stringDerived = tryInferStringPrefix(cmp, left, right);
        if (stringDerived != null) {
            return stringDerived;
        }
        Expression dateFormatDerived = tryInferDateFormat(cmp, left, right, context);
        if (dateFormatDerived != null) {
            return dateFormatDerived;
        }
        Expression fromUnixtimeDerived = tryInferFromUnixtime(cmp, left, right, context);
        if (fromUnixtimeDerived != null) {
            return fromUnixtimeDerived;
        }
        Expression monthsYearsDerived = tryInferMonthsYearsAdd(cmp, left, right, context);
        if (monthsYearsDerived != null) {
            return monthsYearsDerived;
        }
        Expression yearDerived = tryInferYear(cmp, left, right);
        if (yearDerived != null) {
            return yearDerived;
        }
        Expression ceilDerived = tryInferDateCeil(cmp, left, right, context);
        if (ceilDerived != null) {
            return ceilDerived;
        }
        return tryInferDateFloor(cmp, left, right, context);
    }

    // string prefix family: substring(col,1,N) / left(col,N) op strLiteral with codepoints(c) <= N.
    private static Expression tryInferStringPrefix(ComparisonPredicate cmp, Expression left, Expression right) {
        if (!(right instanceof StringLikeLiteral)) {
            return null;
        }
        Expression prefixSource = extractPrefixSource(left);
        if (prefixSource == null) {
            return null;
        }
        int prefixLen = extractPrefixLength(left);
        if (prefixLen <= 0) {
            return null;
        }
        String constValue = ((StringLikeLiteral) right).getStringValue();
        // substring/left length N counts code points; compare against the constant's code-point
        // length so that only constants that fit inside the prefix window derive a sound bound.
        int constCodePoints = constValue.codePointCount(0, constValue.length());
        if (constCodePoints > prefixLen) {
            return null;
        }
        if (cmp instanceof EqualTo) {
            // prefix(col) = c  =>  col has prefix c  =>  c <= col < succ(c).
            // Lower bound c <= col is always sound. The upper bound needs the byte-space successor
            // succ(c), which is only emitted when it is safe under BOTH signed and unsigned byte
            // comparison (i.e. c is pure ASCII) -- see bytePrefixSuccessor. Otherwise we keep just
            // the lower bound (still a correct superset, only less pruning).
            Expression lower = new GreaterThanEqual(prefixSource, new VarcharLiteral(constValue));
            String succ = bytePrefixSuccessor(constValue);
            if (succ == null) {
                return lower;
            }
            return new And(lower, new LessThan(prefixSource, new VarcharLiteral(succ)));
        }
        // Only lower-bound comparisons are sound for a prefix. Upper bounds (<, <=) have no sound
        // bare-column bound (a longer string sharing the prefix can exceed it), so skip them.
        if (!(cmp instanceof GreaterThan) && !(cmp instanceof GreaterThanEqual)) {
            return null;
        }
        // op in {>, >=}: prefix(col) op c  =>  col op c
        return cmp.withChildren(prefixSource, new VarcharLiteral(constValue));
    }

    // Byte-space prefix successor: the smallest string that is strictly greater than every string
    // starting with prefix `s`. Increment the last byte that can be bumped while staying in clean
    // ASCII. Returns null (no upper bound emitted) when a successor cannot be safely built:
    //   - `s` is empty, or
    //   - `s` contains any non-ASCII byte (>= 0x80). Storage-layer string min/max may compare bytes
    //     as signed or unsigned; for pure-ASCII strings (all bytes <= 0x7F) both orders agree, so
    //     the successor is safe. For non-ASCII bytes signed/unsigned diverge, which could make the
    //     upper bound prune rows it must keep -- so we conservatively skip it (keep lower bound only).
    //   - every byte is >= 0x7F, i.e. none can be incremented without leaving clean ASCII (0x7E+1=0x7F
    //     is the largest safe bump; 0x7F+1=0x80 is invalid UTF-8 that new String() would mangle to
    //     U+FFFD). We strip trailing 0x7F bytes and bump the last byte that is <= 0x7E.
    private static String bytePrefixSuccessor(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes) {
            if ((b & 0xFF) >= 0x80) {
                return null;
            }
        }
        int i = bytes.length - 1;
        while (i >= 0 && (bytes[i] & 0xFF) >= 0x7F) {
            i--;
        }
        if (i < 0) {
            return null;
        }
        byte[] out = Arrays.copyOf(bytes, i + 1);
        out[i] = (byte) ((out[i] & 0xFF) + 1);
        return new String(out, StandardCharsets.UTF_8);
    }

    // date floor family: date_trunc(col,'unit') / date(col) / to_date(col) op c.
    // These functions all satisfy the floor property f(col) <= col. For a lower-bound comparison
    // f(col) op c  ==>  col op c  holds for ANY constant c. For equality f(col) = c the exact
    // preimage is col in [c, c + 1 bucket): the upper bound comes from Monotonic.floorUpperBound and
    // is constant-folded here; if it cannot be folded we fail open and keep only the lower bound.
    // from_unixtime is deliberately NOT a floor (representation transform, needs DST/rounding).
    private static Expression tryInferDateFloor(ComparisonPredicate cmp, Expression left,
            Expression right, ExpressionRewriteContext context) {
        Expression floorSource = extractDateFloorSource(left);
        if (floorSource == null) {
            return null;
        }
        if (cmp instanceof GreaterThan || cmp instanceof GreaterThanEqual) {
            // keep the original literal; type coercion casts it to the column type (a date-only
            // constant vs a datetime column means midnight, exactly the floor lower bound).
            return cmp.withChildren(floorSource, right);
        }
        if (cmp instanceof EqualTo && right instanceof Literal && left instanceof Monotonic) {
            // f(col) = c  <=>  col in [c, c + 1 bucket)
            Expression lower = new GreaterThanEqual(floorSource, right);
            // TimeStampTz: BE truncates in SESSION-LOCAL time (function_datetime_floor_ceil.cpp), so a
            // date/week/month/year bucket can be 25h of UTC on a DST fall-back day, but floorUpperBound
            // adds a FIXED UTC bucket (LocalDateTime + 1 unit). That UTC upper bound would sit an hour
            // short of the real local bucket end and prune the fall-back day's last hour. The lower
            // bound col >= c stays sound (floor(col) <= col on the instant axis, zone-independent), so
            // keep only it for a TSTZ column.
            if (isTimeStampTzSlot(floorSource)) {
                return lower;
            }
            Optional<Expression> upperExpr = ((Monotonic) left).floorUpperBound((Literal) right);
            if (!upperExpr.isPresent()) {
                return lower;
            }
            Expression folded = FoldConstantRuleOnFE.evaluate(upperExpr.get(), context);
            if (!(folded instanceof Literal)) {
                return lower; // fail open: cannot fold the upper bound, keep the sound lower bound
            }
            return new And(lower, new LessThan(floorSource, folded));
        }
        return null;
    }

    // date_format(dt, fmt) op c (op in {>, >=}, fmt in the monoFormat whitelist so its output is
    // lexicographically == time ordered, c a string literal)  ==>  dt >= str_to_date(c, fmt).
    // Sound necessary condition (superset), original predicate kept. Only the lower bound and only
    // >/>= are handled; for strict '>' we still emit '>=' str_to_date(c) -- looser but never drops a
    // row. str_to_date bound is constant-folded; fail open (return null) if it cannot be folded.
    private static Expression tryInferDateFormat(ComparisonPredicate cmp, Expression left,
            Expression right, ExpressionRewriteContext context) {
        if (!(cmp instanceof GreaterThan) && !(cmp instanceof GreaterThanEqual)) {
            return null;
        }
        if (!(left instanceof DateFormat) || !(right instanceof StringLikeLiteral)) {
            return null;
        }
        Expression fmt = left.child(1);
        if (!(fmt instanceof StringLikeLiteral)
                || !DateUtils.monoFormat.contains(((StringLikeLiteral) fmt).getValue())) {
            return null;
        }
        Expression src = left.child(0);
        if (!isDateLikeSlot(src)) {
            return null;
        }
        Expression bound = FoldConstantRuleOnFE.evaluate(new StrToDate(right, fmt), context);
        if (!(bound instanceof Literal)) {
            return null; // fail open: cannot fold str_to_date bound
        }
        return new GreaterThanEqual(src, bound);
    }

    // from_unixtime(epochCol) op c (op in {>, >=}, c a datetime-like string literal)  ==>
    // epochCol >= floor(unix_timestamp(c)). from_unixtime maps epoch -> session-local wall clock; on
    // a stretch with no UTC-offset transition that map is a strict affine bijection, so the comparison
    // reverses. Both '>' and '>=' derive '>=': from_unixtime renders the canonical
    // 'yyyy-MM-dd HH:mm:ss', which at the boundary epoch X is strictly greater than a shorter constant
    // like '2026-03-26', so a strict '>' would drop that matching row -- '>=' is the sound superset.
    // The only unsound case is a transition just BELOW the constant's epoch X: a
    // fall-back there lets a smaller epoch also render a local time >= c, so a bare epochCol >= X
    // would wrongly prune it. We therefore require no transition in the guard band [X - GUARD, X]
    // (GUARD = 2 days, well over any real DST/base-offset shift); a transition far below X (e.g.
    // Shanghai 1991 vs a 2026 constant) is harmless because those epochs render years below c and
    // never satisfy the original predicate. Sound necessary condition (superset), original kept.
    // unix_timestamp(string) folds to a DecimalV3(18,6) (sub-second precision); we floor it to whole
    // seconds and compare the BigInt epoch column against that -- flooring only widens a >= lower
    // bound, so it never drops a matching row. Fail open (null) if the bound cannot be folded.
    private static Expression tryInferFromUnixtime(ComparisonPredicate cmp, Expression left,
            Expression right, ExpressionRewriteContext context) {
        if (!(cmp instanceof GreaterThan) && !(cmp instanceof GreaterThanEqual)) {
            return null;
        }
        if (!(left instanceof FromUnixtime) || !(right instanceof StringLikeLiteral)) {
            return null;
        }
        // A 2-arg from_unixtime(epoch, fmt) renders with fmt and is compared as a STRING. The reversal
        // below is sound only when fmt's textual order matches chronological order, i.e. fmt is in the
        // monotonic-format whitelist (same gate FromUnixtime.isMonotonic applies). A non-monotonic fmt
        // such as '%Y-%d-%m' would let 2024-01-31 render as '2024-31-01' >= '2024-02-01' while its epoch
        // is below unix_timestamp('2024-02-01'), so a bare epoch bound would wrongly drop it. The 1-arg
        // form always renders the canonical 'yyyy-MM-dd HH:mm:ss', which is monotonic.
        if (left.arity() == 2) {
            Expression fmt = left.child(1);
            if (!(fmt instanceof StringLikeLiteral)
                    || !DateUtils.monoFormat.contains(((StringLikeLiteral) fmt).getValue())) {
                return null;
            }
        }
        Expression epochSource = left.child(((FromUnixtime) left).getMonotonicFunctionChildIndex());
        if (!(epochSource instanceof Slot) || !epochSource.getDataType().isIntegerLikeType()) {
            return null;
        }
        // reverse the constant: X = unix_timestamp(c). unix_timestamp has no FE-foldable overload for
        // a raw string, so cast c to datetime first: FoldConstantRuleOnFE folds Cast(c) to a datetime
        // literal, then unix_timestamp(datetimeLiteral) to the epoch. The cast uses the session zone,
        // matching from_unixtime's own zone, so X is the exact epoch of the local-time constant c.
        Expression bound = FoldConstantRuleOnFE.evaluate(
                new UnixTimestamp(new Cast(right, DateTimeV2Type.SYSTEM_DEFAULT)), context);
        if (!(bound instanceof NumericLiteral)) {
            return null; // fail open: cannot fold the epoch bound
        }
        long epochX = (long) Math.floor(((Literal) bound).getDouble());
        // DST guard: reject if any offset transition sits in the band just below X (the only place a
        // transition can make the bare-epoch bound unsound). Uses the same primitive as isMonotonic.
        if (DateUtils.hasZoneOffsetTransition(DateUtils.getTimeZone(),
                epochX - DST_GUARD_BAND_SECONDS, epochX)) {
            return null;
        }
        // Always emit '>=' even for a strict '>'. from_unixtime returns the canonical string
        // 'yyyy-MM-dd HH:mm:ss', and the constant c may be a shorter prefix (e.g. '2026-03-26'). At the
        // boundary ts = X = unix_timestamp(c-as-datetime), the rendered 'YYYY-MM-DD 00:00:00' is
        // strictly GREATER than the shorter 'YYYY-MM-DD' string, so the original predicate
        // from_unixtime(ts) > c is TRUE there -- a derived strict ts > X would drop that matching row.
        // '>=' is the sound superset (it also covers the exact-second '>' case, only looser).
        return new GreaterThanEqual(epochSource, new BigIntLiteral(epochX));
    }

    // months_add / years_add / quarters_add (col, k) op c (op in {>, >=}, k an integer literal,
    // col a bare DateV2/DateTimeV2 slot)  ==>  col >= inverse_sub(c, k). These functions are monotone
    // NON-decreasing (end-of-month clamping only creates plateaus -- three January days can map to the
    // same Feb 29 -- but never decreases), so they are NOT floors and reverse via the paired _sub
    // function, not the floor path. Soundness (verified by adversarial calendar enumeration): for the
    // LOWER bound, months_add(col,k) >= c ==> col >= months_sub(c,k); the clamp only makes the round
    // trip undershoot col, so the bound is a sound superset (never drops a matching row). Strict '>'
    // still emits '>=' because plateaus mean '>' cannot be tightened. UPPER bounds (<, <=) are unsound
    // (a plateau's largest preimage is not captured by either _sub or _add) and are excluded. Only
    // DateV2/DateTimeV2 wall-clock columns qualify -- TimeStampTz carries a zone (isDateLikeType() is
    // true for it) so it must be whitelisted out. Pure calendar arithmetic, no DST. The inverse bound
    // is constant-folded; if it overflows the date range plusMonths throws and folding fails, so we
    // fail open (null) and emit nothing -- no manual range guard needed.
    private static Expression tryInferMonthsYearsAdd(ComparisonPredicate cmp, Expression left,
            Expression right, ExpressionRewriteContext context) {
        if (!(cmp instanceof GreaterThan) && !(cmp instanceof GreaterThanEqual)) {
            return null;
        }
        Expression inverse = buildInverseSub(left, right);
        if (inverse == null) {
            return null;
        }
        Expression source = left.child(0);
        if (!(source instanceof Slot)
                || !(source.getDataType().isDateV2Type() || source.getDataType().isDateTimeV2Type())) {
            return null;
        }
        Expression k = left.child(1);
        if (!(k instanceof IntegerLikeLiteral)) {
            return null;
        }
        Expression bound = FoldConstantRuleOnFE.evaluate(inverse, context);
        if (!(bound instanceof Literal)) {
            return null; // fail open: inverse bound not foldable (e.g. date-range overflow)
        }
        // strict '>' can only be relaxed to '>=' because plateaus prevent tightening to '>'
        return new GreaterThanEqual(source, bound);
    }

    // build inverse_sub(c, k) for a months/years/quarters _add expression, or null if `left` is not
    // one of those. The type check gates child access -- these classes guarantee arity 2, whereas a
    // bare slot or unrelated expression may have no children. The constant c is cast to the column
    // type first so the _sub folds on a date literal (matching from_unixtime's cast-then-fold pattern).
    private static Expression buildInverseSub(Expression left, Expression right) {
        if (!(right instanceof Literal)) {
            return null;
        }
        if (!(left instanceof MonthsAdd) && !(left instanceof YearsAdd) && !(left instanceof QuartersAdd)) {
            return null;
        }
        Expression c = new Cast(right, left.child(0).getDataType());
        Expression k = left.child(1);
        if (left instanceof MonthsAdd) {
            return new MonthsSub(c, k);
        }
        if (left instanceof YearsAdd) {
            return new YearsSub(c, k);
        }
        return new QuartersSub(c, k);
    }

    // year(col) op Y (Y an integer literal, col a bare DateV2/DateTimeV2 slot)  ==>  a bare-column
    // range. year() is the only globally monotonic date-component extractor (month/hour/day are
    // periodic, so they have no bare-column preimage). It is a pure calendar-field read on wall-clock
    // types -- no timezone, no DST -- so year(col) = Y  <=>  col in [Y-01-01, (Y+1)-01-01) holds
    // EXACTLY (an equivalent preimage, not a relaxed superset). The half-open interval [b, e) maps the
    // six comparators the same way ClickHouse's toYear preimage does:
    //   = Y  -> col >= b AND col < e      > Y  -> col >= e       >= Y -> col >= b
    //   < Y  -> col <  b                  <= Y -> col <  e
    // (!= is not handled -- it yields an OR, not a pruneable range.) The original predicate is kept
    // (append-only); the exact preimage makes the extra conjunct redundant-but-correct. The derived
    // literals are built directly as date literals (b = Y-01-01, e = (Y+1)-01-01); comparison type
    // coercion casts them to the column type (a date literal vs a DateTimeV2 column is midnight,
    // exactly the bound). Valid year range is 0..9999: b needs Y in [0,9999], e needs Y in [0,9998].
    // If a bound the chosen comparator needs falls out of range we return null (skip the inference).
    private static Expression tryInferYear(ComparisonPredicate cmp, Expression left, Expression right) {
        if (!(left instanceof Year) || !(right instanceof IntegerLikeLiteral)) {
            return null;
        }
        Expression source = left.child(0);
        if (!(source instanceof Slot)
                || !(source.getDataType().isDateV2Type() || source.getDataType().isDateTimeV2Type())) {
            return null;
        }
        int year = ((IntegerLikeLiteral) right).getIntValue();
        // b = Y-01-01 exists for 0 <= Y <= 9999; e = (Y+1)-01-01 exists for Y <= 9998.
        Expression lowerBound = (year >= 0 && year <= 9999) ? new DateV2Literal(year, 1, 1) : null;
        Expression upperBound = (year >= 0 && year <= 9998) ? new DateV2Literal(year + 1, 1, 1) : null;
        if (cmp instanceof GreaterThanEqual) {
            return lowerBound == null ? null : new GreaterThanEqual(source, lowerBound);
        }
        if (cmp instanceof GreaterThan) {
            return upperBound == null ? null : new GreaterThanEqual(source, upperBound);
        }
        if (cmp instanceof LessThan) {
            return lowerBound == null ? null : new LessThan(source, lowerBound);
        }
        if (cmp instanceof LessThanEqual) {
            return upperBound == null ? null : new LessThan(source, upperBound);
        }
        if (cmp instanceof EqualTo) {
            if (lowerBound == null) {
                return null;
            }
            Expression lower = new GreaterThanEqual(source, lowerBound);
            // Y = 9999: no upper bound exists, so keep just the sound lower half (fail open).
            return upperBound == null ? lower : new And(lower, new LessThan(source, upperBound));
        }
        return null;
    }

    // date ceil family: day_ceil(col) / hour_ceil(col) / ... op c (NO origin form -- see below).
    // Ceil functions satisfy f(col) >= col, the mirror of floor. For an upper-bound comparison
    // f(col) op c  ==>  col op c  holds for ANY constant c (col <= f(col) op c). For equality
    // f(col) = c the exact preimage is col in (c - 1 bucket, c]: the lower bound comes from
    // Monotonic.ceilLowerBound and is constant-folded here; if it cannot be folded we fail open and
    // keep only the upper bound col <= c. Mirror of tryInferDateFloor.
    private static Expression tryInferDateCeil(ComparisonPredicate cmp, Expression left,
            Expression right, ExpressionRewriteContext context) {
        Expression ceilSource = extractDateCeilSource(left);
        if (ceilSource == null) {
            return null;
        }
        if (cmp instanceof LessThan || cmp instanceof LessThanEqual) {
            // keep the original literal; type coercion casts it to the column type. col <= f(col) op c.
            return cmp.withChildren(ceilSource, right);
        }
        if (cmp instanceof EqualTo && right instanceof Literal && left instanceof Monotonic) {
            // f(col) = c  <=>  col in (c - 1 bucket, c]
            Expression upper = new LessThanEqual(ceilSource, right);
            // TimeStampTz: BE ceils in SESSION-LOCAL time, so the bucket can be 25h of UTC on a DST
            // fall-back day, but ceilLowerBound subtracts a FIXED UTC bucket. That UTC lower bound would
            // sit an hour inside the real local bucket start and prune the fall-back day's first hour.
            // The upper bound col <= c stays sound (ceil(col) >= col on the instant axis,
            // zone-independent), so keep only it for a TSTZ column. Mirror of the floor case.
            if (isTimeStampTzSlot(ceilSource)) {
                return upper;
            }
            Optional<Expression> lowerExpr = ((Monotonic) left).ceilLowerBound((Literal) right);
            if (!lowerExpr.isPresent()) {
                return upper;
            }
            Expression folded = FoldConstantRuleOnFE.evaluate(lowerExpr.get(), context);
            if (!(folded instanceof Literal)) {
                return upper; // fail open: cannot fold the lower bound, keep the sound upper bound
            }
            return new And(new GreaterThan(ceilSource, folded), upper);
        }
        return null;
    }

    // return the inner column if expr is a ceil function (Monotonic.isCeil(): f(col) >= col) whose
    // range argument is a bare date-like slot; otherwise null. Origin forms (a 3rd arg, or a 2nd arg
    // that is not a positive integer-literal period) are REJECTED: with a user origin the
    // month/quarter/year ceil can round DOWN (day-of-month clamp fights ceil), breaking f(col) >= col
    // and making the upper bound unsound. Only the no-origin forms (arity 1, or arity 2 with a
    // positive integer literal period) are let through -- there origin defaults to the epoch/first-day
    // and the ceil property holds.
    private static Expression extractDateCeilSource(Expression expr) {
        if (!(expr instanceof Monotonic) || !((Monotonic) expr).isCeil()) {
            return null;
        }
        if (!ceilHasNoOriginAndValidPeriod(expr)) {
            return null;
        }
        Expression arg = expr.child(((Monotonic) expr).getMonotonicFunctionChildIndex());
        return isDateLikeSlot(arg) ? arg : null;
    }

    // return the inner column if expr is a floor function (Monotonic.isFloor(): f(col) <= col, e.g.
    // date_trunc(col,'unit') / date(col) / to_date(col) / day_floor(col[,period[,origin]])) whose
    // range argument is a bare date-like slot; otherwise null. Driven by the isFloor() declaration,
    // NOT by hardcoded function types -- adding a new floor function only requires isFloor().
    // Unlike ceil, a user ORIGIN is fine for floor: time_round FLOOR always rounds toward the bucket
    // boundary <= col (function_datetime_floor_ceil.cpp step = diff - delta_inside_period, no +period),
    // so f(col) <= col holds for any origin and the lower-bound reversal stays sound. The ONLY gate
    // for the *_floor family (DateCeilFloorMonotonic) is the integer PERIOD: a non-positive or
    // non-literal period is rejected by BE at runtime (period < 1), so deriving a bound then could
    // prune to empty and mask that error. date_trunc's 2nd arg is a string unit and Date/ToDate are
    // unary, so neither is a DateCeilFloorMonotonic and both pass through untouched.
    private static Expression extractDateFloorSource(Expression expr) {
        if (!(expr instanceof Monotonic) || !((Monotonic) expr).isFloor()) {
            return null;
        }
        if (expr instanceof DateCeilFloorMonotonic && !floorHasValidPeriod(expr)) {
            return null;
        }
        Expression arg = expr.child(((Monotonic) expr).getMonotonicFunctionChildIndex());
        return isDateLikeSlot(arg) ? arg : null;
    }

    // Ceil gate (DateCeilFloorMonotonic): only the no-origin forms are safe to reverse into an upper
    // bound, because a user origin can round the ceil DOWN past col (breaking f(col) >= col).
    //   - arity 1 (col)                    : ok.
    //   - arity 2 (col, positiveIntPeriod) : ok (a date-like 2nd arg is an origin -> rejected; a
    //                                        non-positive/non-literal period -> rejected, see below).
    //   - arity >= 3 (col, period, origin) : rejected (has an origin).
    private static boolean ceilHasNoOriginAndValidPeriod(Expression expr) {
        if (expr.arity() == 1) {
            return true;
        }
        if (expr.arity() == 2) {
            return isPositiveIntegerLiteral(expr.child(1));
        }
        return false; // arity >= 3: has an origin
    }

    // Floor gate (DateCeilFloorMonotonic): an origin is allowed (floor stays <= col for any origin);
    // the only requirement is that an integer PERIOD, when present, is a positive literal so it does
    // not mask a BE runtime error (period < 1) by pruning to empty. The period vs origin distinction
    // is by TYPE: an integer-typed 2nd arg is a period (must be a positive literal -- a dynamic column
    // or a non-positive value cannot be validated at plan time), a date-like 2nd arg is an origin.
    //   - arity 1 (col)                        : ok.
    //   - arity 2 (col, intPeriod)             : ok iff the period is a positive integer literal.
    //   - arity 2 (col, dateOrigin)            : ok (origin is date-like, not an integer period).
    //   - arity 3 (col, intPeriod, dateOrigin) : ok iff the period is a positive integer literal.
    private static boolean floorHasValidPeriod(Expression expr) {
        if (expr.arity() >= 2 && expr.child(1).getDataType().isIntegerLikeType()) {
            return isPositiveIntegerLiteral(expr.child(1));
        }
        return true;
    }

    private static boolean isPositiveIntegerLiteral(Expression e) {
        return e instanceof IntegerLikeLiteral && ((IntegerLikeLiteral) e).getIntValue() > 0;
    }

    private static boolean isDateLikeSlot(Expression expr) {
        return expr instanceof Slot && expr.getDataType().isDateLikeType();
    }

    // a TimeStampTz-typed slot. BE floor/ceil on such a column truncates in session-local time, so a
    // date/week/month/year bucket is not a fixed UTC width (a DST fall-back day is 25h). The =-preimage
    // opposite bound (built by adding/subtracting a FIXED UTC bucket) is therefore unsound for TSTZ and
    // is skipped; the same-direction bound stays sound on the instant axis.
    private static boolean isTimeStampTzSlot(Expression expr) {
        return expr instanceof Slot && expr.getDataType().isTimeStampTzType();
    }

    // return the inner column-like source if expr is substring(src, 1, N) or left(src, N) whose
    // source is a string-typed slot; otherwise null.
    private static Expression extractPrefixSource(Expression expr) {
        if (expr instanceof Substring) {
            Substring substring = (Substring) expr;
            Expression position = substring.getPosition();
            if (!(position instanceof IntegerLikeLiteral)
                    || ((IntegerLikeLiteral) position).getIntValue() != 1) {
                return null;
            }
            return isStringSlot(substring.getSource()) ? substring.getSource() : null;
        }
        if (expr instanceof Left) {
            Left left = (Left) expr;
            return isStringSlot(left.child(0)) ? left.child(0) : null;
        }
        return null;
    }

    // return the prefix length N as an int, or -1 if it is not a positive integer literal.
    private static int extractPrefixLength(Expression expr) {
        if (expr instanceof Substring) {
            Substring substring = (Substring) expr;
            if (!substring.getLength().isPresent()) {
                return -1;
            }
            Expression length = substring.getLength().get();
            return length instanceof IntegerLikeLiteral ? ((IntegerLikeLiteral) length).getIntValue() : -1;
        }
        if (expr instanceof Left) {
            Expression length = ((Left) expr).child(1);
            return length instanceof IntegerLikeLiteral ? ((IntegerLikeLiteral) length).getIntValue() : -1;
        }
        return -1;
    }

    private static boolean isStringSlot(Expression expr) {
        return expr instanceof Slot && expr.getDataType() instanceof CharacterType;
    }
}
