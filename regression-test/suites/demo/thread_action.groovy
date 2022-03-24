def (_, elapsedMillis) = timer {
    /**
     * the default max thread num is 10, you can specify by 'actionParallel' param.
     * e.g. ./run-regression-test.sh --run someSuite -actionParallel 10
     */
    def future1 = thread("threadName1") {
        sleep(200)
        sql"select 1"
    }

    // create new thread but not specify name
    def future2 = thread {
        sleep(200)
        sql "select 2"
    }

    def future3 = thread("threadName3") {
        sleep(200)
        sql "select 3"
    }

    def future4 = thread {
        sleep(200)
        sql "select 4"
    }

    // equals to combineFutures([future1, future2, future3, future4]), which [] is a Iterable<ListenableFuture>
    def combineFuture = combineFutures(future1, future2, future3, future4)
    // or you can use lazyCheckThread action(see lazyCheck_action.groovy), and not have to check exception from futures.
    List<List<List<Object>>> result = combineFuture.get()
    assertEquals(result[0][0][0], 1)
    assertEquals(result[1][0][0], 2)
    assertEquals(result[2][0][0], 3)
    assertEquals(result[3][0][0], 4)
}
assertTrue(elapsedMillis < 600)


// you can use qt action in thread action, and you **MUST** specify different tag,
// testing framework can compare different qt result in different order.
lazyCheckThread {
    sleep(100)
    qt_diffrent_tag1 "select 100"
}

lazyCheckThread("lazyCheckThread2") {
    qt_diffrent_tag2 "select 100"
}