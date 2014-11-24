package topology;

import spout.WordGeneratorSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.WordCounter;
import bolt.WordNormalizer;

public class ExampleTopology {

	public static void main(String[] args) {
		try{
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("word-reader", new WordGeneratorSpout());
			builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
			builder.setBolt("word-counter", new WordCounter() , 2).
				fieldsGrouping("word-normalizer" , new Fields("word"));
			
			Config conf = new Config();
			conf.put("wordsFile", args[0]);
			conf.setDebug(true);
			
			conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING , 1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Example", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
