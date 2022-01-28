package io.lixiang.fl.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountJob {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = env.readTextFile("data/wc.data");

        stringDataSource
                .flatMap(new FlatMapFunction<String, String>() {

                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] words = s.split(",");
                        for (String word :
                                words) {
                            collector.collect(word);
                        }
                    }
                })
                .filter(s -> StringUtils.isNotBlank(s))
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                })
                //As a contradiction of Stream which uses keyBy(), use groupBy() in Batch.
                .groupBy(0)
                .sum(1).print();
        // Do not need to run env.execute() in Batch.
    }
}
