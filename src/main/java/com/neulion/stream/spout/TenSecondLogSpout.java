package com.neulion.stream.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.neulion.stream.MessageBean;

/**
 * @author hellojinjie
 * @Date 2013-1-16
 */
public class TenSecondLogSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private static final Log log = LogFactory.getLog(TenSecondLogSpout.class);
	
	private static String LOG_FILENAME = "data/10secondsimulator.log";
	
	private SpoutOutputCollector collector;
	private ArrayList<List<Object>> logData = new ArrayList<List<Object>>();
	
	/**
	 */
	private long lastEmitTime;
	
	/**
	 */
	private long clock = 0;
	
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			this.prepareData();
		} catch (Exception e) {
			log.fatal("", e);
		}
		this.lastEmitTime = System.currentTimeMillis() / 1000;
	}

	/**
	 */
	public void nextTuple() {
		long currentTime = System.currentTimeMillis() / 1000;
		if (currentTime == this.lastEmitTime) {
			try {
				TimeUnit.MILLISECONDS.sleep(100);
				return;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		
		long count = currentTime - this.lastEmitTime;
		for (int i = 0; i < count; i++) {
			List<Object> tuples = this.logData.get((int) ((clock + i) % this.logData.size()));
			for (Object o : tuples) {
				collector.emit(new Values(o));
			}
		}
		this.lastEmitTime = currentTime;
		this.clock = this.clock + count;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

	/**
	 * 
	 * @throws Exception
	 */
	private void prepareData() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(LOG_FILENAME));
		String oneLine = br.readLine();
		if (oneLine == null) {
			br.close();
			throw new Exception("!!!");
		}
		
		String currentTime = "";
		ArrayList<Object> messages = null;
		int totalLines = 0;
		while (oneLine != null) {
			String time = oneLine.substring(0, oneLine.indexOf("\t"));
			log.info("currentTime: " + currentTime);
			log.info("time: " + time);
			if (!currentTime.equals(time)) {
				if (messages != null) {
					this.logData.add(messages);
				}
				messages = new ArrayList<Object>();
				currentTime = time;
			}
			
			MessageBean message = MessageBean.parse(oneLine);	
			totalLines++;
			messages.add(message);
			oneLine = br.readLine();
		}
		log.info("Lines in ten second log: " + totalLines);
		logData.add(messages);
		br.close();
	}
}
