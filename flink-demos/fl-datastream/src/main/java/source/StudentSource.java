package source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * RichSourceFunction parallelism is 1
 */
public class StudentSource extends RichSourceFunction<Student> {
    private boolean running = true;
    // MySQLConnection Connection String

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // initialize the MySQLConnection
    }

    @Override
    public void close() throws Exception {
        super.close();
        // close the MySQLConnection
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        // query the MySQLConnection
    }

    @Override
    public void cancel() {

    }
}
