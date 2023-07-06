package org.apache.doris.scheduler.executor;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

public class SqlJobExecutor implements JobExecutor{

    @Expose
    @Getter
    @Setter
    @SerializedName(value = "a")
    private int a;
    
    @Getter
    @Setter
    @SerializedName(value = "sql")
    private String sql;
    @Override
    public Object execute() {
        // parse sql
        a++;
        System.out.println("sql job executor"+sql+" "+a++);
        return null;
    }
    
}
