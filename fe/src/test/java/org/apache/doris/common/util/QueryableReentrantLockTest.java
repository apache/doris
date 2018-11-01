package org.apache.doris.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class QueryableReentrantLockTest {

    private QueryableReentrantLock lock = new QueryableReentrantLock(true);

    @Test
    public void test() throws InterruptedException {

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }, "thread1");

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    if (!lock.tryLock(1000, TimeUnit.MILLISECONDS)) {
                        Thread owner = lock.getOwner();
                        Assert.assertEquals("thread1", owner.getName());

                        System.out.println(Util.dumpThread(owner, 10));

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }, "thread2");

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }

}
