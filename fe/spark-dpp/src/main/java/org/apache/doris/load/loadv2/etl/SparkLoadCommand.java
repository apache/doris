package org.apache.doris.load.loadv2.etl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class SparkLoadCommand {

    private SparkLoadCommand() {
    }

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
            description = "路径",
            required = true
    )
    private String configFile;

    public String getConfigFile() {
        return configFile;
    }
}
