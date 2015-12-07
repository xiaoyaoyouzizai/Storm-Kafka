package test.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;

import java.util.Map;

public class ExtractFieldsBolt extends BaseRichBolt {
	private static final long serialVersionUID = -3608935066469056646L;
	private OutputCollector collector;

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(",");
		if (words.length >= 24) {
			String game = Utils.trim(words[1]);
			String user_id = Utils.trim(words[7]);
			int order_type = Integer.parseInt(Utils.trim(words[11]));
			float amount = Float.parseFloat(Utils.trim(words[13]));
			String time = words[23].substring(1, 17);

			collector.emit(input, new Values("", game + "," + user_id + "," + order_type + "," + amount + "," + time));
		}
		collector.ack(input);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("game", "user_id", "order_type",
		// "amount", "time"));
		declarer.declare(
				new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE));
	}

}
