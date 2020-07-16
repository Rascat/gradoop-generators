import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.datagen.transactions.foodbroker.FoodBroker;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;


public class Runner {
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

    BufferedInputStream stream = new BufferedInputStream(Runner.class.getResourceAsStream("foodbroker/config.json"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    String jsonString = reader.lines().collect(Collectors.joining("\n"));

    FoodBrokerConfig foodBrokerConfig = FoodBrokerConfig.fromJSONString(jsonString);
    foodBrokerConfig.setScaleFactor(Integer.parseInt(args[0]));
    FoodBroker foodBroker = new FoodBroker(env, cfg, foodBrokerConfig);

    GraphCollection collection = foodBroker.execute();

    DataSink sink = new CSVDataSink(args[1], cfg);
    sink.write(collection);

    env.execute();
  }
}
