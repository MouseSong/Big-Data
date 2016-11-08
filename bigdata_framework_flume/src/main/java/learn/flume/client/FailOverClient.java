package learn.flume.client;

import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * 带有失效转移功能的客户端.
 * @author zhangdong
 * @createtime 2016-11-08
 * @location peking
 * */
public class FailOverClient {

	private RpcClient client;
	private Properties props;
	
	public void init(){
		props = new Properties();
		props.put("client.type", "default_failover");
		props.put("hosts", "h1 h2");
		String host1 = "hbase:5140";
		String host2 = "master:5140";
		props.put("hosts.h1", host1);
		props.put("hosts.h2", host2);
		client = RpcClientFactory.getInstance(props);
	}
	
	public void sendDataToFlume(String data){
		Event event = EventBuilder.withBody(data.getBytes());
	    
		try {
			client.append(event);
		} catch (EventDeliveryException e) {
			client.close();
			client = null;
			client = RpcClientFactory.getInstance(props);
		}
	}
	
	public void cleanup(){
		client.close();
	}
}
