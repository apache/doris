suite("timer_action", "demo") {
    def (sumResult, elapsedMillis) = timer {
        long sum = 0
        (1..10000).each {sum += it}
        sum // return value
    }

    // you should convert GString to String because the log of GString not contains the correct file name,
    // e.g. (NativeMethodAccessorImpl.java:-2) - sum: 50005000, elapsed: 47 ms
    // so, invoke toString and then we can get the log:
    // (timer_action.groovy:10) - sum: 50005000, elapsed: 47 ms
    logger.info("sum: ${sumResult}, elapsed: ${elapsedMillis} ms".toString())
}
