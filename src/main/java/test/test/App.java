package test.test;

import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

// stage 2
public class App {
	private static final String KAFKA_READER = "kafka-reader";
	private static final String SPLIT_BOLT = "split-bolt";
	private static final String SUM_BOLT = "sum-bolt";
	private static final String FORWARD_TO_KAFKA = "forward-to-kafka";

	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			return;
		}

		String topologyName = args[0];
		ExecutorTaskConf spoutConf = new ExecutorTaskConf(args[1]);
		ExecutorTaskConf splitBoltConf = new ExecutorTaskConf(args[2]);
		ExecutorTaskConf sumBoltConf = new ExecutorTaskConf(args[3]);
		ExecutorTaskConf outBoltConf = new ExecutorTaskConf(args[4]);
		int workers = Integer.parseInt(args[5]);

		//
		BrokerHosts hosts = new ZkHosts(Utils.zkConnString);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, Utils.KAFKA_TOPIC_EXTRACT, "/" + Utils.KAFKA_TOPIC_EXTRACT,
				UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		//
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(KAFKA_READER, kafkaSpout, spoutConf.getExecutors()).setNumTasks(spoutConf.getTasks());

		//
		SplitFieldsBolt splitBolt = new SplitFieldsBolt();
		builder.setBolt(SPLIT_BOLT, splitBolt, splitBoltConf.getExecutors()).setNumTasks(splitBoltConf.getTasks())
				.shuffleGrouping(KAFKA_READER);

		//
		SumOrdersBolt sumBolt = new SumOrdersBolt();
		builder.setBolt(SUM_BOLT, sumBolt, sumBoltConf.getExecutors()).setNumTasks(sumBoltConf.getTasks())
				.fieldsGrouping(SPLIT_BOLT, new Fields("game", "order_type", "time"));

		KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
				.withTopicSelector(new DefaultTopicSelector("test"))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

		builder.setBolt(FORWARD_TO_KAFKA, kafkaBolt, outBoltConf.getExecutors()).setNumTasks(outBoltConf.getTasks())
				.shuffleGrouping(SUM_BOLT);

		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.1.144:9092");
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		Config config = new Config();
		config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
		config.setNumWorkers(workers);

		StormSubmitter.submitTopologyWithProgressBar(topologyName, config, builder.createTopology());
	}
}
