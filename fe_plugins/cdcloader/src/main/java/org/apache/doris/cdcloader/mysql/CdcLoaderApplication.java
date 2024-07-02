package org.apache.doris.cdcloader.mysql;


import org.apache.doris.cdcloader.common.factory.DataSource;
import org.apache.doris.cdcloader.common.factory.SourceReaderFactory;
import org.apache.doris.cdcloader.mysql.config.LoadContext;
import org.apache.doris.cdcloader.mysql.reader.SourceReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.Arrays;

@SpringBootApplication
@EnableConfigurationProperties
@ServletComponentScan
public class CdcLoaderApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        System.out.println("args: " + Arrays.asList(args));
        SourceReader sourceReader = SourceReaderFactory.createSourceReader(DataSource.MYSQL);
        LoadContext context = LoadContext.getInstance();
        context.setSourceReader(sourceReader);
        SpringApplication.run(CdcLoaderApplication.class, args);
    }
}
