package customer.source;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.codehaus.jackson.map.ObjectMapper;
import org.hyperic.sigar.Sigar;

import customer.bean.HostInfo;
import customer.source.sigar.SigarMain;

public class SigarSource extends AbstractSource implements Configurable, EventDrivenSource  {

	private ScheduledExecutorService executorService;
	
	public void configure(Context context) {
		
	}

	@Override
	public synchronized void start() {
		// TODO Auto-generated method stub
		//super.start();
		//启动线程向  ip:port 发送数据
		//executorService = Executors.newSingleThreadExecutor();
		ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
		try {
			Socket socket = new Socket("hbase",4141);
			//executorService.submit(new SigarTask(socket));
			ses.scheduleAtFixedRate(new SigarTask(socket), 1, 100, TimeUnit.MICROSECONDS);
		} catch (Exception e) {
			e.printStackTrace();
			executorService.shutdown();
		}
		
	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		executorService.shutdown();
		//停止线程
	}
}

class SigarTask implements Runnable{
	private Socket socket;
	public SigarTask(Socket socket){
		this.socket = socket;
	}
	public void run() {
		try{
			// TODO Auto-generated method stub
			SigarMain sm = new SigarMain(new Sigar());
			HostInfo info = sm.getHostInfo();
			ObjectMapper om = new ObjectMapper();
			String result = om.writeValueAsString(info);
			
			OutputStream out = socket.getOutputStream();
		    PrintWriter pw = new PrintWriter(out);
		    pw.write(result);
		    pw.flush();
		    //out.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
