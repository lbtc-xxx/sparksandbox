package sandbox;

import org.apache.spark.SparkConf;
import org.junit.Test;

public class MainTest {

    @Test
    public void executeWorks() {
        new Main(new SparkConf().setAppName("JavaSparkPi").setMaster("local")).execute(2);
    }
}
