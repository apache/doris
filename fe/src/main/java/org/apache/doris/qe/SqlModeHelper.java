package org.apache.doris.qe;


import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.doris.common.AnalysisException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlModeHelper {
    private static final Logger LOG = LogManager.getLogger(SqlModeHelper.class);

    /* Bits for different SQL MODE modes, you can add custom SQL MODE here */
    public static final Long MODE_REAL_AS_FLOAT = 1L;
    public static final Long MODE_PIPES_AS_CONCAT = 2L;
    public static final Long MODE_ANSI_QUOTES = 4L;
    public static final Long MODE_IGNORE_SPACE = 8L;
    public static final Long MODE_NOT_USED = 16L;
    public static final Long MODE_ONLY_FULL_GROUP_BY = 32L;
    public static final Long MODE_NO_UNSIGNED_SUBTRACTION = 64L;
    public static final Long MODE_NO_DIR_IN_CREATE = 128L;
    public static final Long MODE_ANSI = 1L << 18;
    public static final Long MODE_NO_AUTO_VALUE_ON_ZERO = 1L << 19;
    public static final Long MODE_NO_BACKSLASH_ESCAPES = 1L << 20;
    public static final Long MODE_STRICT_TRANS_TABLES = 1L << 21;
    public static final Long MODE_STRICT_ALL_TABLES = 1L << 22;


    public final static Long MODE_DEFAULT = 1L << 32;
    public final static Long MODE_LAST = 1L << 33;

    public final static Long MODE_ALLOWED_MASK =
            (MODE_REAL_AS_FLOAT | MODE_PIPES_AS_CONCAT | MODE_ANSI_QUOTES |
                    MODE_IGNORE_SPACE | MODE_NOT_USED | MODE_ONLY_FULL_GROUP_BY |
                    MODE_NO_UNSIGNED_SUBTRACTION | MODE_NO_DIR_IN_CREATE | MODE_ANSI |
                    MODE_NO_AUTO_VALUE_ON_ZERO | MODE_NO_BACKSLASH_ESCAPES |
                    MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES | MODE_DEFAULT);

    private final static ImmutableMap<String, Long> sqlModeSet =
            ImmutableMap.<String, Long>builder()
                    .put("REAL_AS_FLOAT", MODE_REAL_AS_FLOAT)
                    .put("PIPES_AS_CONCAT", MODE_PIPES_AS_CONCAT)
                    .put("ANSI_QUOTES", MODE_ANSI_QUOTES)
                    .put("IGNORE_SPACE", MODE_IGNORE_SPACE)
                    .put("NOT_USED", MODE_NOT_USED)
                    .put("ONLY_FULL_GROUP_BY", MODE_ONLY_FULL_GROUP_BY)
                    .put("NO_UNSIGNED_SUBTRACTION", MODE_NO_UNSIGNED_SUBTRACTION)
                    .put("NO_DIR_IN_CREATE", MODE_NO_DIR_IN_CREATE)
                    .put("ANSI", MODE_ANSI)
                    .put("NO_AUTO_VALUE_ON_ZERO", MODE_NO_AUTO_VALUE_ON_ZERO)
                    .put("NO_BACKSLASH_ESCAPES", MODE_NO_BACKSLASH_ESCAPES)
                    .put("STRICT_TRANS_TABLES", MODE_STRICT_TRANS_TABLES)
                    .put("STRICT_ALL_TABLES", MODE_STRICT_ALL_TABLES)
                    .put("DEFAULT", MODE_DEFAULT)
                    .build();




    public static String parseValue(Long sqlMode) throws AnalysisException {
        if ((sqlMode & ~MODE_ALLOWED_MASK) != 0) {
            throw new AnalysisException("Unsupported sql mode found while parsing value");
        }

        List<String> names = new ArrayList<String>();
        for (Map.Entry<String, Long> mode : getSupportedSqlMode().entrySet()) {
            if ((sqlMode & mode.getValue()) != 0) {
                names.add(mode.getKey());
            }
        }

        return Joiner.on(',').join(names);
    }

    public static Long parseString(String sqlMode) throws AnalysisException {
        Long value = 0L;
        List<String> names =
                Splitter.on(',').trimResults().omitEmptyStrings().splitToList(sqlMode);
        for (String key : names) {
            if (getSupportedSqlMode().containsKey(key)) {
                value |= getSupportedSqlMode().get(key);
            } else {
                throw new AnalysisException("Unsupported sql mode found while parsing string");
            }
        }

        return value;
    }



    public static ImmutableMap<String, Long> getSupportedSqlMode() {
        return sqlModeSet;
    }

}
