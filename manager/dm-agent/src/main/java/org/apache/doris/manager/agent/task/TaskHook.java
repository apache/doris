package org.apache.doris.manager.agent.task;

public abstract class TaskHook<T extends TaskDesc> {
    public abstract void onSuccess(T taskDesc);
}
