package test.test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SplitFieldsBolt extends BaseRichBolt {
	private static final long serialVersionUID = 6109347028131120124L;
	private OutputCollector collector;

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(",");
		if (words.length == 5) {
			String game = (words[0]);
			String user_id = (words[1]);
			int order_type = Integer.parseInt(words[2]);
			float amount = Float.parseFloat(words[3]);
			String time = words[4];

			collector.emit(input, new Values(game, user_id, order_type, amount, time));
		}
		collector.ack(input);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("game", "user_id", "order_type", "amount", "time"));
	}

}
