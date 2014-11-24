package bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer implements IRichBolt {

	private TopologyContext context;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
		if (line != null && line.isEmpty()) {
			String[] words = line.split("\\s+");
			for (String word : words) {
				word = word.trim();
				if (!word.isEmpty()){
					word = word.toLowerCase();
					List a = new ArrayList();
					a.add(input);
					collector.emit(a , new Values(word));
				}
			}
		}
		
		collector.ack(input);
		
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
