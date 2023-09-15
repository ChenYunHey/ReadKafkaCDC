import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class Read {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(32);
        //env.setParallelism(4);

        env.enableCheckpointing(60000);
        String kafkaBootstrapServers = "192.168.0.156:6667,192.168.0.172:6667,192.168.0.130:6667";
        String kafkaTopic = "t_test_table2";
        String outputPath = "s3://dmetasoul-bucket/yunhe/k8s/data";
        //String outputPath = "/tmp/kafka/914";
        KafkaSource kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId("912")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> strategy = WatermarkStrategy.noWatermarks();
        DataStreamSource<String> streamSource =  env.fromSource(kafkaSource, strategy,"connect kafka CDC ");

//        DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy
//                .create()
//                .withMaxPartSize(1024*1024*1024)
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10).getSeconds())
                                .withInactivityInterval(Duration.ofSeconds(10).getSeconds())
                                .withMaxPartSize(1024*1024*1024)
                                .build())
                .build();
        //streamSource.writeAsText(outputPath).setParallelism(320);
        streamSource.sinkTo(sink);
        env.execute();
    }
}
