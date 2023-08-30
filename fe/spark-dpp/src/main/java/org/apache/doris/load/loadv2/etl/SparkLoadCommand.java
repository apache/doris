package org.apache.doris.load.loadv2.etl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.Serializable;

public class SparkLoadCommand implements Serializable {

    public static SparkLoadCommand parse(String[] args) {
        SparkLoadCommand command = new SparkLoadCommand();
        JCommander commander = JCommander
                .newBuilder()
                .programName(SparkLoadCommand.class.getName())
                .addObject(command)
                .build();
        commander.parse(args);
        return command;
    }

    @Parameter(
            names = {"--config-file", "-c"},
            description = "配置文件路径",
            required = true
    )
    private String configFile;

    @Parameter(
            names = {"--enable-hive"},
            description = "使能hive"
    )
    private boolean enableHive = false;

    @Parameter(
            names = {"--debug-output-file-groups-path"},
            description = "把源文件输出到指定路径",
            required = false
    )
    private String _debugFileGroupPath;

    public String getConfigFile() {
        return configFile;
    }

    public boolean getEnableHive() {
        return enableHive;
    }

    public String getDebugFileGroupPath() {
        return _debugFileGroupPath;
    }

}
