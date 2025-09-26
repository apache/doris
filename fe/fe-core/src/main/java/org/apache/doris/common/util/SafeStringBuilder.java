package org.apache.doris.common.util;

import lombok.Getter;
import org.apache.doris.common.profile.Profile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SafeStringBuilder {
    private StringBuilder builder = new StringBuilder();
    @Getter
    private long maxCapacity;
    @Getter
    private boolean truncated = false;
    private Logger LOG = LogManager.getLogger(Profile.class);

    public SafeStringBuilder() {
        this(Integer.MAX_VALUE);
    }

    public SafeStringBuilder(int _maxCapacity) {
        if (_maxCapacity < 16) {
            LOG.warn("SafeStringBuilder max capacity {} must be greater than 16", _maxCapacity);
            _maxCapacity = 16;
        }
        this.maxCapacity = _maxCapacity - 16;
    }

    public SafeStringBuilder append(String str) {
        if (!truncated) {
            if (builder.length() + str.length() <= maxCapacity) {
                builder.append(str);
            } else {
                LOG.warn("Append str truncated, builder length(): {}, str length: {}, max capacity: {}",
                        builder.length(), str.length(), maxCapacity);
                builder.append(str, 0, (int)(maxCapacity - builder.length()));
                markTruncated();
            }
        }
        return this;
    }

    public SafeStringBuilder append(Object obj) {
        return append(String.valueOf(obj));
    }

    public int length() {
        return builder.length();
    }

    public String toString() {
        if (truncated) {
            return builder.toString() + "\n...[TRUNCATED]";
        }
        return builder.toString();
    }

    private void markTruncated() {
        truncated = true;
        LOG.warn("SafeStringBuilder exceeded max capacity {}", maxCapacity);
    }
}
