package learn.kafka.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Kafka 简单消费者客户端.
 * */
public class KafkaConsumer {
	
	private final String topic;
	private final ConsumerConnector consumer;
	private ExecutorService executor;
	private static ConsumerConfig consumerConf;

	public KafkaConsumer(String topic){
		consumer = Consumer.createJavaConsumerConnector(consumerConf);
		this.topic = topic;
	}
	static {
		Properties props = new Properties();
		props.put("zookeeper.connect", "hbase:2181");
		props.put("group.id", "test_group");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		consumerConf = new ConsumerConfig(props);
	}
	
	public void run(int taskNum){
		Map<String, Integer> topicTaskNums = new HashMap<String, Integer>();
	    topicTaskNums.put(topic, taskNum);
	    
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = 
	    		consumer.createMessageStreams(topicTaskNums);
	    		//new HashMap<String, List<KafkaStream<byte[], byte[]>>>();
	    
	    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
	    
	    
	    executor = Executors.newFixedThreadPool(taskNum);
	    
	    int threadNum = 0;
	    for(KafkaStream<byte[], byte[]> stream : streams){
	    	executor.submit(new ConsumerMsgTask(stream, threadNum));
	    	threadNum ++;
	    }
	    
	}
	
	public void quit(){
		if(Objects.nonNull(consumer)){
			consumer.shutdown();
		}
		if(Objects.nonNull(executor)){
			executor.shutdown();
		}
	}
	
	public static void main(String[] args)throws Exception {
		KafkaConsumer consumer = new KafkaConsumer("TEST");
		consumer.run(1);
		
		TimeUnit.SECONDS.sleep(2);
	}
	
}


class ConsumerMsgTask implements Runnable{
	
	private KafkaStream<byte[], byte[]> stream;
	private int threadNumber;
	public ConsumerMsgTask(KafkaStream<byte[], byte[]> stram, int threadNumber){
		this.stream = stram;
		this.threadNumber = threadNumber;
	}

	public void run() {
		// TODO Auto-generated method stub
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while(iterator.hasNext()){
			System.out.println("Thread " + threadNumber + ":" + new String(iterator.next().message()));
		}
	}
	
}