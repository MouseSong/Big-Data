package customer.source;

import java.io.OutputStream; 
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.codehaus.jackson.map.ObjectMapper;

public class SigarSource extends AbstractSource implements Configurable, EventDrivenSource  {

	private ScheduledExecutorService executorService;
	private String agentNo;
	private String hostName;
	private String port;
	public void configure(Context context) {
		agentNo = context.getString("no");
		hostName = context.getString("hostname");
		port = context.getString("port");
	}

	@Override
	public synchronized void start() {
		System.out.println("Sigar Source Started");
		// TODO Auto-generated method stub
		//super.start();
		//启动线程向  ip:port 发送数据
	    executorService = Executors.newSingleThreadScheduledExecutor();
		try {
			Socket socket = new Socket(hostName,Integer.parseInt(port));
			executorService.scheduleAtFixedRate(new SigarTask(socket, agentNo), 1, 100, TimeUnit.MICROSECONDS);
		} catch (Exception e) {
			e.printStackTrace();
			executorService.shutdown();
		}
	}

	@Override
	public synchronized void stop() {
		executorService.shutdown();
		//停止线程
		System.out.println("SigarSource shutdown");
	}
}

class SigarTask implements Runnable{
	private static int counter = 0;
	private Socket socket;
	private String agentNo;
	public SigarTask(Socket socket, String agentNo){
		this.socket = socket;
		this.agentNo = agentNo;
	}
	public void run() {
		try{
			// TODO Auto-generated method stub
			/*SigarMain sm = new SigarMain(new Sigar());
			System.out.println(new Sigar().getCpu().getTotal());
			System.out.println("Main;" + sm);
			HostInfo info = sm.getHostInfo();
			System.out.println(info);*/
			
			Map<String, String> tempData = new HashMap<String, String>();
			tempData.put("name", "zhangdong");
			tempData.put("position", "peking");
			tempData.put("time", new Date().getTime() + "");
			tempData.put("no", agentNo);
			tempData.put("msgNo","" + (counter ++));
			tempData.put("message",UUID.randomUUID().toString());
			
			ObjectMapper om = new ObjectMapper();
			//System.out.println(tempData);
			String result = om.writeValueAsString(tempData);
			//System.out.println("HostInfo:" + result);
			OutputStream out = socket.getOutputStream();
		    PrintWriter pw = new PrintWriter(out);
		    pw.write(result);
		    pw.flush();
		    //System.out.println("Write Success.");
		    //out.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
