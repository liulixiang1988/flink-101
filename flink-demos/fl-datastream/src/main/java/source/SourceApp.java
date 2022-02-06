package source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
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
        // testCollectionSource(env);
        // testKafka(env);

        //testCustomizeSource(env);
        testCustomizeSourceV2(env);
        env.execute();
    }
    private static void testCustomizeSourceV2(StreamExecutionEnvironment env) {

        DataStreamSource<Access> accessDataStreamSource = env.addSource(new AccessSourceV2()).setParallelism(2);
        System.out.println(accessDataStreamSource.getParallelism());
        accessDataStreamSource.print();
    }
    private static void testCustomizeSource(StreamExecutionEnvironment env) {

        DataStreamSource<Access> accessDataStreamSource = env.addSource(new AccessSource());
        System.out.println(accessDataStreamSource.getParallelism());
        accessDataStreamSource.print();
    }


    private static void testKafka(StreamExecutionEnvironment env) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("flink")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        System.out.println(kafka_source.getParallelism());
        kafka_source.print();
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
