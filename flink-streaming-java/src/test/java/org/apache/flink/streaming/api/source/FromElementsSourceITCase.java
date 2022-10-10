package org.apache.flink.streaming.api.source;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.connector.source.splittable.FromElementsSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** {@link FromElementsSource} test. */
public class FromElementsSourceITCase extends TestLogger {
    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER =
            new MiniClusterResource(new MiniClusterResourceConfiguration.Builder().build());

    private StreamExecutionEnvironment env;

    @Before
    public void setup() {
        System.out.println("SETUP");
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    @Test
    public void testRunSource() throws JobExecutionException, InterruptedException {
        System.out.println("RUN");
        ArrayList<String> strings = Lists.newArrayList("one", "two", "3");
        DataStreamSource<String> source =
                env.fromCollectionNew(strings, TypeInformation.of(String.class));

        source.sinkTo(new PrintSink<>());

        MINI_CLUSTER.getMiniCluster().executeJobBlocking(env.getStreamGraph().getJobGraph());
    }

    @Test
    public void testRunSourceWithCheckpoint() throws Exception {
        env.enableCheckpointing(50);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 200L));

        List<String> collect =
                IntStream.range(1, 1000000)
                        .boxed()
                        .map(String::valueOf)
                        .collect(Collectors.toList());

        DataStreamSource<String> source =
                env.fromCollectionNew(collect, TypeInformation.of(String.class));

        source.sinkTo(new TestSink<>());

        MiniCluster miniCluster = MINI_CLUSTER.getMiniCluster();
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        CompletableFuture<JobSubmissionResult> resultFuture = miniCluster.submitJob(jobGraph);
        Thread.sleep(100L);
        miniCluster
                .triggerCheckpoint(jobGraph.getJobID())
                .thenRun(
                        () -> {
                            miniCluster.terminateTaskManager(0);
                            try {
                                miniCluster.startTaskManager();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });

        resultFuture.join();
    }

    /** todo. */
    public static class TestSink<T> implements Sink<T> {

        @Override
        public SinkWriter<T> createWriter(InitContext context) throws IOException {
            return new TestSinkWriter<>();
        }
    }

    /** todo. */
    public static class TestSinkWriter<T> implements SinkWriter<T> {

        @Override
        public void write(T element, Context context) throws IOException, InterruptedException {}

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {}

        @Override
        public void close() throws Exception {}
    }
}
