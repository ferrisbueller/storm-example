package bolt;

import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {

	
	private TopologyContext context ;
	private OutputCollector collector;
	Map<String , Integer> wordCounts;
	String name;
	Integer id;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.context = context ;
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		if (!wordCounts.containsKey(word)) {
			wordCounts.put(word, 1);
		} else {
			Integer prevCount = wordCounts.get(word);
			wordCounts.put(word , prevCount +  1);
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		System.out.println("Cleanup happening");
		for (Entry<String, Integer> wordCount : wordCounts.entrySet()) {
			System.out.println("Word Count for " + wordCount.getKey() + " is " + wordCount.getValue());
		}
		System.out.println("Cleanup completed");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// DECLARE NOTHING
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
