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

package org.apache.doris.cloud.stage;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Brace expansion and wildcard detection for the globs accepted by {@code COPY INTO}.
 *
 * <p>Ported from hadoop's {@code org.apache.hadoop.fs.GlobExpander} and
 * {@code org.apache.hadoop.fs.GlobPattern} (both Apache-2.0), which {@link StageUtil} used
 * directly until fe-core dropped its hadoop source imports. {@code StageUtil} already carries
 * local copies of the neighbouring {@code org.apache.hadoop.fs.Globber} helpers, so these two
 * complete the set.
 *
 * <p>Semantics are preserved exactly, down to compiling with re2j the way hadoop's
 * {@code GlobPattern} does. That matters twice over:
 * <ul>
 *   <li>{@link #hasWildcard} decides whether a path component is a literal prefix or the point
 *       where wildcard matching starts, which is what {@code StageUtil.analyzeGlob} turns into
 *       the object-store listing prefix. Widening or narrowing it lists the wrong keys.</li>
 *   <li>Both entry points reject malformed globs, and {@code analyzeGlob} surfaces that as a
 *       {@code DdlException}. A laxer validator would silently accept e.g. {@code a[b} and copy
 *       from an unintended prefix instead of failing the statement.</li>
 * </ul>
 *
 * <p>Only what {@code StageUtil} calls is ported: hadoop's glob <em>matching</em> is not, since
 * the compiled pattern is used here purely to validate. Note this is a different glob dialect
 * from {@code org.apache.doris.common.GlobRegexUtil}, which treats {@code {} } as literal
 * characters and has no notion of brace groups.
 */
final class GlobPatterns {

    private static final char BACKSLASH = '\\';

    private GlobPatterns() {
    }

    /**
     * Expands the glob into a set of patterns none of which has a slash inside a curly bracket
     * pair, e.g. {@code p{a/b,c/d}s} becomes {@code [pa/bs, pc/ds]}. Brace groups without a slash
     * are left alone for {@link #hasWildcard} to handle per component.
     *
     * @throws IOException if the glob ends with a dangling escape character
     */
    static List<String> expand(String filePattern) throws IOException {
        List<String> fullyExpanded = new ArrayList<>();
        List<StringWithOffset> toExpand = new ArrayList<>();
        toExpand.add(new StringWithOffset(filePattern, 0));
        while (!toExpand.isEmpty()) {
            StringWithOffset path = toExpand.remove(0);
            List<StringWithOffset> expanded = expandLeftmost(path);
            if (expanded == null) {
                fullyExpanded.add(path.string);
            } else {
                toExpand.addAll(0, expanded);
            }
        }
        return fullyExpanded;
    }

    /**
     * Whether the glob contains a wildcard, i.e. any of {@code * ? [ {}. Escaped characters and
     * the closing {@code ] } } do not count, matching hadoop's {@code GlobPattern.hasWildcard()}.
     *
     * @throws IOException if the glob is malformed, mirroring the {@code GlobFilter} constructor
     */
    static boolean hasWildcard(String glob) throws IOException {
        try {
            return compile(glob);
        } catch (PatternSyntaxException e) {
            throw new IOException("Illegal file pattern: " + e.getMessage(), e);
        }
    }

    /**
     * Translates the glob to a regex exactly as hadoop's {@code GlobPattern.set} does and compiles
     * it for its validation side effect, returning whether a wildcard was seen.
     */
    private static boolean compile(String glob) {
        StringBuilder regex = new StringBuilder();
        int setOpen = 0;
        int curlyOpen = 0;
        int len = glob.length();
        boolean hasWildcard = false;

        for (int i = 0; i < len; i++) {
            char c = glob.charAt(i);

            switch (c) {
                case BACKSLASH:
                    if (++i >= len) {
                        error("Missing escaped character", glob, i);
                    }
                    regex.append(c).append(glob.charAt(i));
                    continue;
                case '.':
                case '$':
                case '(':
                case ')':
                case '|':
                case '+':
                    // escape regex special chars that are not glob special chars
                    regex.append(BACKSLASH);
                    break;
                case '*':
                    regex.append('.');
                    hasWildcard = true;
                    break;
                case '?':
                    regex.append('.');
                    hasWildcard = true;
                    continue;
                case '{': // start of a group
                    regex.append("(?:"); // non-capturing
                    curlyOpen++;
                    hasWildcard = true;
                    continue;
                case ',':
                    regex.append(curlyOpen > 0 ? '|' : c);
                    continue;
                case '}':
                    if (curlyOpen > 0) {
                        // end of a group
                        curlyOpen--;
                        regex.append(")");
                        continue;
                    }
                    break;
                case '[':
                    if (setOpen > 0) {
                        error("Unclosed character class", glob, i);
                    }
                    setOpen++;
                    hasWildcard = true;
                    break;
                case '^': // ^ inside [...] can be unescaped
                    if (setOpen == 0) {
                        regex.append(BACKSLASH);
                    }
                    break;
                case '!': // [! needs to be translated to [^
                    regex.append(setOpen > 0 && '[' == glob.charAt(i - 1) ? '^' : '!');
                    continue;
                case ']':
                    // Many set errors like [][] could not be easily detected here,
                    // as []], []-] and [-] are all valid POSIX glob and java regex.
                    // We'll just let the regex compiler do the real work.
                    setOpen = 0;
                    break;
                default:
            }
            regex.append(c);
        }

        if (setOpen > 0) {
            error("Unclosed character class", glob, len);
        }
        if (curlyOpen > 0) {
            error("Unclosed group", glob, len);
        }
        // Compiled for validation only: re2j rejects character-class errors that the scan above
        // deliberately leaves to it (see the ']' case), and hadoop surfaced those the same way.
        // DOTALL is not about matching here -- it never runs -- but re2j echoes the flags into the
        // PatternSyntaxException text, which ends up verbatim in the user-facing DdlException.
        Pattern.compile(regex.toString(), Pattern.DOTALL);
        return hasWildcard;
    }

    /**
     * Expands the leftmost outer curly bracket pair that contains a slash.
     *
     * @return the expansions, or null when there is no such pair
     */
    private static List<StringWithOffset> expandLeftmost(StringWithOffset filePatternWithOffset)
            throws IOException {
        String filePattern = filePatternWithOffset.string;
        int leftmost = leftmostOuterCurlyContainingSlash(filePattern, filePatternWithOffset.offset);
        if (leftmost == -1) {
            return null;
        }
        int curlyOpen = 0;
        StringBuilder prefix = new StringBuilder(filePattern.substring(0, leftmost));
        StringBuilder suffix = new StringBuilder();
        List<String> alts = new ArrayList<>();
        StringBuilder alt = new StringBuilder();
        StringBuilder cur = prefix;
        for (int i = leftmost; i < filePattern.length(); i++) {
            char c = filePattern.charAt(i);
            if (cur == suffix) {
                cur.append(c);
            } else if (c == '\\') {
                i++;
                if (i >= filePattern.length()) {
                    throw new IOException("Illegal file pattern: "
                            + "An escaped character does not present for glob "
                            + filePattern + " at " + i);
                }
                c = filePattern.charAt(i);
                cur.append(c);
            } else if (c == '{') {
                if (curlyOpen++ == 0) {
                    alt.setLength(0);
                    cur = alt;
                } else {
                    cur.append(c);
                }
            } else if (c == '}' && curlyOpen > 0) {
                if (--curlyOpen == 0) {
                    alts.add(alt.toString());
                    alt.setLength(0);
                    cur = suffix;
                } else {
                    cur.append(c);
                }
            } else if (c == ',') {
                if (curlyOpen == 1) {
                    alts.add(alt.toString());
                    alt.setLength(0);
                } else {
                    cur.append(c);
                }
            } else {
                cur.append(c);
            }
        }
        List<StringWithOffset> exp = new ArrayList<>();
        for (String string : alts) {
            exp.add(new StringWithOffset(prefix + string + suffix, prefix.length()));
        }
        return exp;
    }

    /**
     * @return the index of the leftmost opening curly bracket containing a slash, or -1
     */
    private static int leftmostOuterCurlyContainingSlash(String filePattern, int offset) throws IOException {
        int curlyOpen = 0;
        int leftmost = -1;
        boolean seenSlash = false;
        for (int i = offset; i < filePattern.length(); i++) {
            char c = filePattern.charAt(i);
            if (c == '\\') {
                i++;
                if (i >= filePattern.length()) {
                    throw new IOException("Illegal file pattern: "
                            + "An escaped character does not present for glob "
                            + filePattern + " at " + i);
                }
            } else if (c == '{') {
                if (curlyOpen++ == 0) {
                    leftmost = i;
                }
            } else if (c == '}' && curlyOpen > 0) {
                if (--curlyOpen == 0 && leftmost != -1 && seenSlash) {
                    return leftmost;
                }
            } else if (c == '/' && curlyOpen > 0) {
                seenSlash = true;
            }
        }
        return -1;
    }

    private static void error(String message, String pattern, int pos) {
        throw new PatternSyntaxException(String.format("%s at pos %d", message, pos), pattern);
    }

    private static class StringWithOffset {
        private final String string;
        private final int offset;

        StringWithOffset(String string, int offset) {
            this.string = string;
            this.offset = offset;
        }
    }
}
