package test.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
//import redis.clients.jedis.Jedis;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;

public class SumOrdersBolt extends BaseRichBolt {
	private static final Log LOG = LogFactory.getLog(SumOrdersBolt.class);
	private static final long serialVersionUID = -5290911935744994810L;

	// private Jedis jedisSum;
	private OutputCollector collector;
	// private Map<String, Float> sumMap;
	private String current_key;
	private Map<String, Integer> countMap;

	public void execute(Tuple input) {
		String game = input.getString(0);
		String user_id = input.getString(1);
		int order_type = input.getInteger(2);
		float amount = input.getFloat(3);
		String time = input.getString(4);

		LOG.info("RECV[splitter -> sum] " + game + "," + user_id + "," + order_type + "," + amount + "," + time);

		String key = game + "_" + order_type + "_" + time;

		// Float sum = sumMap.get(key);
		// if (sum == null) {
		// sum = 0f;
		// }
		// sum += amount;
		// sumMap.put(key, sum);

		Integer count = countMap.get(key);
		if (count == null) {
			count = 0;
		}
		count++;
		countMap.put(key, count);

		// jedisSum.set(key, String.valueOf(sum));
		// jedisSum.set("count_" + key, String.valueOf(count));
		if (!key.equals(current_key)) {
			collector.emit(input, new Values("", game + "," + order_type + "," + count + "," + time));
			current_key = key;
		}
		collector.ack(input);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// sumMap = new HashMap<String, Float>();
		countMap = new HashMap<String, Integer>();
		// jedisSum = new Jedis("192.168.1.141");
		// jedisSum.select(15);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(
				new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE));
	}

	public void cleanup() {
		// jedisSum.close();
	}
}
