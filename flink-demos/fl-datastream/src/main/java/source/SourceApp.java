package source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class SourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // there are some alternative methods to create environments, like
        // env = StreamExecutionEnvironment.createLocalEnvironment();
        // env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 9999, "user", "password");
        // env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration.fromMap(null));

        // testParallel(env);
        testCollectionSource(env);

        env.execute();
    }

    private static void testParallel(StreamExecutionEnvironment env) {
        // set parallelism for the job
        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
        System.out.println("socket stream: " + stream.getParallelism());

        SingleOutputStreamOperator<String> filter = stream.filter("test"::equals);
        // set parallelism for the operator
        filter.setParallelism(6);
        System.out.println("filter stream: " + filter.getParallelism());

        filter.print();
    }

    private static void testCollectionSource(StreamExecutionEnvironment env) {
        // set parallelism for the job
        env.setParallelism(2);

        DataStreamSource<Long> stream = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println("socket stream: " + stream.getParallelism());

        SingleOutputStreamOperator<Long> filter = stream.filter(x -> x % 2 == 0);
        // set parallelism for the operator
        filter.setParallelism(6);
        System.out.println("filter stream: " + filter.getParallelism());

        filter.print();
    }
}
