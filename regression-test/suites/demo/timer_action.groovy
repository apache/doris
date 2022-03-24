def (sumResult, elapsedMillis) = timer {
    long sum = 0
    (1..10000).each {sum += it}
    sum // return value
}

logger.info("sum: ${sumResult}, elapsed: ${elapsedMillis} ms")
