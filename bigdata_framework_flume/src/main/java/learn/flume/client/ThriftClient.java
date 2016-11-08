package learn.flume.client;

import java.nio.charset.Charset;
import java.util.Objects;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * Thrift client java api.
 * @author zhangdong
 * @createtime 2016-11-07
 * */
public class ThriftClient {

	public static void main(String[] args) {
		MyThriftClient thriftClient = new MyThriftClient();
		thriftClient.init("hbase", 4142);
		for(int i = 0;i < 10; i ++){
			thriftClient.sendDataToFlume("Hello flume, thrift" + i);
		}
		
		thriftClient.cleanup();
	}
}

class MyThriftClient{
	
	private String hostname;
	private int port;
	private RpcClient client;
	
	
	public void init(String hostname, int port){
		this.hostname = hostname;
		this.port = port;
		//init thrift client
		client = RpcClientFactory.getThriftInstance(hostname, port);
	}
	//send data to thrift source
	public void sendDataToFlume(String data){
		//create event
		Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
		try {
		if(Objects.nonNull(client))
			//send 
			client.append(event);
		} catch (EventDeliveryException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			client.close();
			client = null;
			client = RpcClientFactory.getThriftInstance(hostname, port);
		}
	}
	
	public void cleanup(){
		if(Objects.nonNull(client))
			client.close();
	}
	
	
}
