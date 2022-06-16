package org.apache.doris.nereids.pattern;

/**
 * Types for all Pattern type.
 * <p>
 * There are four types of Pattern for pattern matching:
 * 1. NORMAL:      normal pattern matching, e.g. match by operatorId or class type.
 * 2. ANY:         match any operator, return a plan when matched.
 * 3. MULTI:       match multiple children operators, that we don't know how many children exist.
 *                 only use as the last child pattern, and can not use as the top pattern.
 *                 return some children plan with real plan type when matched.
 * 4. GROUP:       match a group plan, only use in a pattern's children, and can not use as the top pattern.
 *                 return a GroupPlan when matched.
 * 5. MULTI_GROUP: match multiple group plan, that we don't know how many children group exist.
 *                 only use in a pattern's children, so can not use as the top pattern.
 *                 return some children GroupPlan when matched.
 * </p>
 */
public enum PatternType {
    NORMAL,
    ANY,
    MULTI,
    GROUP,
    MULTI_GROUP
}
