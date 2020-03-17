package org.gradoop;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.codehaus.jettison.json.JSONException;
import org.gradoop.flink.datagen.transactions.foodbroker.FoodBroker;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class Main {
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

    String configPath = Main.class.getResource("/foodbroker/config.json").getFile();
    FoodBrokerConfig config = FoodBrokerConfig.fromFile(configPath);

    FoodBroker broker = new FoodBroker(env, cfg, config);
     GraphCollection collection = broker.execute();

    CSVDataSink sink = new CSVDataSink("out/foodbroker-csv", cfg);
    collection.writeTo(sink);

    env.execute();
  }
}
