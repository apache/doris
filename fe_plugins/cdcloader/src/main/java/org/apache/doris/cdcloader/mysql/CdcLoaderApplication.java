package org.apache.doris.cdcloader.mysql;


import org.apache.doris.cdcloader.common.factory.DataSource;
import org.apache.doris.cdcloader.common.factory.SplitFactory;
import org.apache.doris.cdcloader.mysql.config.LoaderOptions;
import org.apache.doris.cdcloader.mysql.constants.LoadConstants;
import org.apache.doris.cdcloader.mysql.loader.LoadContext;
import org.apache.doris.cdcloader.mysql.loader.SplitAssigner;
import org.apache.doris.cdcloader.mysql.loader.SplitReader;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Preconditions;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@EnableConfigurationProperties
@ServletComponentScan
public class CdcLoaderApplication extends SpringBootServletInitializer {
    private static final List<String> EMPTY_KEYS = Collections.singletonList("password");

    public static void main(String[] args) {
        System.out.println("args: " + Arrays.asList(args));
        MultipleParameterTool parameter = MultipleParameterTool.fromArgs(args);
        Long jobId = Long.parseLong(parameter.get("job-id"));
        String frontends = parameter.get("frontends");
        Map<String, String> configMap = getConfigMap(parameter, "cdc-conf");
        String sourceType = configMap.get(LoadConstants.DB_SOURCE_TYPE);
        Preconditions.checkNotNull(sourceType);
        DataSource dataSource = DataSource.valueOf(sourceType.toUpperCase());
        Preconditions.checkNotNull(dataSource, "Unsupported source type: " + sourceType);

        SpringApplication.run(CdcLoaderApplication.class, args);
        LoaderOptions loaderOptions = new LoaderOptions(jobId, frontends, configMap);
        SplitAssigner splitAssigner = SplitFactory.createSplitAssigner(dataSource, loaderOptions);
        splitAssigner.prepare();
        splitAssigner.start();
        SplitReader splitReader = SplitFactory.createSplitReader(dataSource, loaderOptions, splitAssigner);
        LoadContext context = LoadContext.getInstance();
        context.setSplitReader(splitReader);
        context.setSplitAssigner(splitAssigner);
        context.setLoaderOptions(loaderOptions);
        splitReader.start();
    }

    private static Map<String, String> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            System.out.println(
                "Can not find key ["
                    + key
                    + "] from args: "
                    + params.toMap().toString()
                    + ".\n");
            return null;
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            } else if (kv.length == 1 && EMPTY_KEYS.contains(kv[0])) {
                map.put(kv[0], "");
                continue;
            }

            System.out.println("Invalid " + key + " " + param + ".\n");
            return null;
        }
        return map;
    }
}
