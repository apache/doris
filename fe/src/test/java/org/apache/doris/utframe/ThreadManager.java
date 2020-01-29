package org.apache.doris.utframe;

import com.google.common.collect.Maps;

import java.util.Map;

public class ThreadManager {
    private static class SingletonHolder {
        private static final ThreadManager INSTANCE = new ThreadManager();
    }

    public static ThreadManager getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public static class ThreadAlreadyExistException extends Exception {
        ThreadAlreadyExistException(String msg) {
            super(msg);
        }
    }

    private Map<String, Thread> threadMap = Maps.newConcurrentMap();

    public void registerThread(Thread thread) throws ThreadAlreadyExistException {
        if (threadMap.putIfAbsent(thread.getName(), thread) != null) {
            throw new ThreadAlreadyExistException("");
        }
    }

    public Thread getThread(String name) {
        return threadMap.get(name);
    }

    public Thread removeThread(String name) {
        return threadMap.remove(name);
    }
}
