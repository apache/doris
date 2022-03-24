/***** 1. lazy check exceptions *****/

// will not throw exception immediately
def result = lazyCheck {
    sql "a b c d e d" // syntax error
}
assertTrue(result == null)

result = lazyCheck {
    sql "select 100"
}
assertEquals(result[0][0], 100)

logger.info("You will see this log")

// if you not clear the lazyCheckExceptions, and then,
// after this suite execute finished, the syntax error in the lazyCheck action will be thrown.
lazyCheckExceptions.clear()


/***** 2. lazy check futures *****/

// start new thread and lazy check future
def futureResult = lazyCheckThread {
    sql "a b c d e d"
}
assertTrue(futureResult instanceof java.util.concurrent.Future)

logger.info("You will see this log too")

// if you not clear the lazyCheckFutures, and then,
// after this suite execute finished, the syntax error in the lazyCheckThread action will be thrown.
lazyCheckFutures.clear()
