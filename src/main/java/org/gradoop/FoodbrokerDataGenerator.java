package org.gradoop;

import org.apache.commons.cli.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.datagen.transactions.foodbroker.FoodBroker;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class FoodbrokerDataGenerator {
  public static void main(String[] args) throws Exception {
    FoodbrokerOptions options = new FoodbrokerOptions(args);
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

    String configPath = FoodbrokerDataGenerator.class.getResource("/foodbroker/config.json").getFile();
    FoodBrokerConfig config = FoodBrokerConfig.fromFile(configPath);
    config.setScaleFactor(options.scaleFactor());

    FoodBroker broker = new FoodBroker(env, cfg, config);
    GraphCollection collection = broker.execute();

    CSVDataSink sink = new CSVDataSink(options.out(), cfg);
    collection.writeTo(sink);

    env.execute();
  }

  private static class FoodbrokerOptions {

    private CommandLine cmd;

    private final String OPT_SCALE = "scale";
    private final String OPT_OUT = "out";

    private FoodbrokerOptions(String[] args) throws ParseException {
      Options options = createOptions();
      CommandLineParser parser = new DefaultParser();
      this.cmd = parser.parse(options, args);
    }

    private int scaleFactor() {
      return Integer.parseInt(cmd.getOptionValue(OPT_SCALE));
    }

    private String out() {
      return cmd.getOptionValue(OPT_OUT);
    }

    private Options createOptions() {
      Options options = new Options();

      Option scale = Option.builder(OPT_SCALE)
        .required(true)
        .hasArg(true)
        .desc("Scale factor: can be int in range [1 - 10000]")
        .build();

      Option out = Option.builder(OPT_OUT)
        .required(true)
        .hasArg(true)
        .desc("Output path")
        .build();

      return options
        .addOption(scale)
        .addOption(out);
    }
  }
}
