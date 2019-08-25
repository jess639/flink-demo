package flinkKafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaConsumer010 {

	public static void main(String[] args) throws Exception {
	   final Properties pro = new Properties();
		pro.put("bootstrap.servers","allen:9092") ;
        pro.put("group.id","jess10") ;
        pro.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        pro.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		 
		DataStream<String> input = env.addSource(new FlinkKafkaConsumer010<String>(
								"abc",
								new SimpleStringSchema(),
								pro) ) ;
		input.print();
		input.addSink(new FlinkKafkaProducer010<String>( "bcd",
						new SimpleStringSchema(),
						pro));
		env.execute("Kafka 0.10 Example");
	}
}
