package sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private final SparkConf sparkConf;

    Main(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    public void execute(int slices) {
        System.out.println("start: " + LocalDateTime.now());

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        System.out.println("context initialized: " + LocalDateTime.now());

        int n = 100000 * slices;
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        final JavaRDD<Integer> map = dataSet.map((i1) -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y < 1) ? 1 : 0;
        });

        final Integer count = map.reduce((i1, i2) -> i1 + i2);

        System.out.println("Pi is roughly " + 4.0 * count / n);

        System.out.println("end: " + LocalDateTime.now());

        jsc.stop();
    }

    public static void main(String[] args) {
        new Main(new SparkConf().setAppName("JavaSparkPi"))
                .execute((args.length == 1) ? Integer.parseInt(args[0]) : 2);
    }
}
