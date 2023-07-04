package org.apache.doris.scheduler.common;


import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public enum IntervalUnit {

    SECOND("second", 0L, TimeUnit.SECONDS::toMillis),
    MINUTE("minute", 0L, TimeUnit.MINUTES::toMillis),
    HOUR("hour", 0L, TimeUnit.HOURS::toMillis),
    DAY("day", 0L, TimeUnit.DAYS::toMillis),
    WEEK("week", 0L, v -> TimeUnit.DAYS.toMillis(v * 7));
    private final String unit;

    public String getUnit() {
        return unit;
    }

    public static IntervalUnit fromString(String unit) {
        for (IntervalUnit u : IntervalUnit.values()) {
            if (u.unit.equalsIgnoreCase(unit)) {
                return u;
            }
        }
        return null;
    }

    private final Object defaultValue;

    private final Function<Long, Long> converter;

    IntervalUnit(String unit, Long defaultValue, Function<Long, Long> converter) {
        this.unit = unit;
        this.defaultValue = defaultValue;
        this.converter = converter;
    }

    IntervalUnit getByName(String name) {
        return Arrays.stream(IntervalUnit.values())
                .filter(config -> config.getUnit().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown configuration " + name));
    }

    public Long getParameterValue(Long param) {
        return (Long) (param != null ? converter.apply(param) : defaultValue);
    }
}
