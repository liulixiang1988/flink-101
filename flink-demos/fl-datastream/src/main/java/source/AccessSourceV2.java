package source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class AccessSourceV2 implements ParallelSourceFunction<Access> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        String[] domain = {"www.baidu.com", "www.google.com", "www.taobao.com", "www.jd.com", "www.qq.com", "www.sina.com.cn"};
        Random random = new Random();

        while (running) {
            for (int i = 0; i < 10; i++) {
                Access access = new Access();
                access.setTime(System.currentTimeMillis());
                access.setDomain(domain[random.nextInt(domain.length)]);
                access.setTraffic(random.nextInt(1000));
                ctx.collect(access);
            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
