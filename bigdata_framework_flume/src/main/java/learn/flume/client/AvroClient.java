package learn.flume.client;

import java.nio.charset.Charset;
import java.util.Objects;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import learn.flume.util.Util;

/**
 * Flume avro java Client.
 * @author zhangdong
 * @createtime 2016-11-07
 * @location peking deshengmen
 * */
public class AvroClient {
    public static void main(String[] args) {
	     	MyAvroClient avroClient = new MyAvroClient();
	     	avroClient.init("hbase", 4141);
	     	
	     	String data = "Hello Flume";
	     	for(int i = 0;i < 1000;i ++){
	     		avroClient.sendDataToFlume(data + " - " + i);
	     	}
	     	
	     	avroClient.cleanup();
	     	
	}
}

class MyAvroClient{
	
	private String hostname;
	private int port;
	private RpcClient client;
	
	public void init(String hostname, int port){
		this.hostname = hostname;
		this.port = port;
		//init avro client
		client = RpcClientFactory.getDefaultInstance(hostname, port);
	}
	
	public void sendDataToFlume(String data){
		//create event
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
	    //send event to avro source
		try{
		   if(Objects.nonNull(client))
			   client.append(event);
		}catch(EventDeliveryException e){
			client.close();
			client = null;
			client = RpcClientFactory.getInstance(Util.getAvroProperties());
		}
	}
	
	public void cleanup(){
		if(Objects.nonNull(client))
			client.close();
	}
}
