package spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordGeneratorSpout implements IRichSpout{

	
	private boolean completed;
	FileReader reader = null;
	SpoutOutputCollector collector = null;
	TopologyContext context = null;
	
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			this.context = context;
			this.reader = new FileReader(new File(conf.get("wordsFile").toString()));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return;
		}
		
		String line = null;
		
		BufferedReader lineReader = new BufferedReader(reader);
		
		try {
			
			if ((line = lineReader.readLine()) != null) {
				collector.emit(new Values(line), line);
			}
			
		} catch (IOException e) {
			
		} finally {
			completed = true;
		}
		
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("ACK :: " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL :: " + msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
