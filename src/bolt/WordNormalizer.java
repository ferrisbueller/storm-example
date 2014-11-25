package bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import clojure.main;
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
		System.out.println("Inside execute for word normalizer");
		String line = input.getString(0);
		System.out.println("GOT LINE AS INSIDE NORMALIZER :: " + line);
		if (line != null && !line.isEmpty()) {
			String[] words = line.split("\\s+");
			for (String word : words) {
				System.out.println("TRYING TO PROCESS WORD :: " + word);
				word = word.trim();
				if (!word.isEmpty()){
					word = word.toLowerCase();
					List a = new ArrayList();
					a.add(input);
					System.out.println("EMITTING WORD :: " + word);
					collector.emit(a , new Values(word));
				}
			}
		}
		
		collector.ack(input);
		
		System.out.println("Leaving execute function for word normalizer");
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

	public static void main(String[] args) {
		String line = "Always look on the bright side of life";
		String[] splits = line.split("\\s+");
		for (String string : splits) {
			System.out.println(string);
		}
	}
	
}
