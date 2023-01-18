package org.apache.doris.regression.util

class SuiteUtils {
    static <T> Tuple2<T, Long> timer(Closure<T> actionSupplier) {
        long startTime = System.currentTimeMillis()
        T result = actionSupplier.call()
        long endTime = System.currentTimeMillis()
        return [result, endTime - startTime]
    }
}
