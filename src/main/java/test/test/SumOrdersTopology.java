package test.test;

import backtype.storm.topology.TopologyBuilder;

import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

public class SumOrdersTopology {

	private static final String KAFKA_READER = "kafka-reader";
	private static final String EXTRACT_BOLT = "extract-bolt";
	private static final String FORWARD_TO_KAFKA = "forward-to-kafka";
	private static final String TOPIC_NAME = "orders";

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			return;
		}

		String topologyName = args[0];
		ExecutorTaskConf spoutConf = new ExecutorTaskConf(args[1]);
		ExecutorTaskConf splitBoltConf = new ExecutorTaskConf(args[2]);
		ExecutorTaskConf outBoltConf = new ExecutorTaskConf(args[3]);
		int workers = Integer.parseInt(args[4]);

		//
		BrokerHosts hosts = new ZkHosts(Utils.zkConnString);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, TOPIC_NAME, "/" + TOPIC_NAME, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		//
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(KAFKA_READER, kafkaSpout, spoutConf.getExecutors()).setNumTasks(spoutConf.getTasks());

		ExtractFieldsBolt splitBolt = new ExtractFieldsBolt();
		builder.setBolt(EXTRACT_BOLT, splitBolt, splitBoltConf.getExecutors()).setNumTasks(splitBoltConf.getTasks())
				.shuffleGrouping(KAFKA_READER);

		KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>()
				.withTopicSelector(new DefaultTopicSelector(Utils.KAFKA_TOPIC_EXTRACT))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

		builder.setBolt(FORWARD_TO_KAFKA, kafkaBolt, outBoltConf.getExecutors()).setNumTasks(outBoltConf.getTasks())
				.shuffleGrouping(EXTRACT_BOLT);

		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.1.142:9092");
		props.put("acks", "1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		Config config = new Config();
		config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
		config.setNumWorkers(workers);

		StormSubmitter.submitTopologyWithProgressBar(topologyName, config, builder.createTopology());
		// } else {
		// LocalCluster cluster = new LocalCluster();
		//
		// cluster.submitTopology(SUM_TOPOLOGY, config,
		// builder.createTopology());
		// Thread.sleep(10000);
		// cluster.killTopology(SUM_TOPOLOGY);
		// cluster.shutdown();
		// }
	}
}
