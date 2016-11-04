package learn.kafka.client;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 利用JavaAPI实现kafka生产者
 * @author zhangdong
 * @createtime 2016-11-04
 * @location peking deshengmen
 * */
public class KafkaProducer {

	//private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    private static Properties props = new Properties(); 
	static{
		props.put("metadata.broker.list", "hbase:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
	}
    
	private void sendMessage(String message){
		//ProducerConfig pconf = new ProducerConfig(props);
		ProducerConfig pconf = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(pconf);
		long start=System.currentTimeMillis();
		
		String ip = "192.168.11.132";
		for(int i = 0;i < 100;i ++){
			long currentTime = new Date().getTime();
			String msg = ip + "-" + currentTime + ":" + message;
			KeyedMessage<String, String> kmsg = new KeyedMessage<String, String>("TEST",ip, msg); 
		    producer.send(kmsg);
		}
		System.out.println("耗时:" + (System.currentTimeMillis() - start));
		producer.close();
	}
	
	public static void main(String[] args) {
		KafkaProducer producer = new KafkaProducer();
		producer.sendMessage("this is test message");
	}

}
