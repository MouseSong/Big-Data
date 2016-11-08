package custom.test;

import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;

/**
 * 测试Sigar，获取系统信息
 * @author zhangdong
 * @createtime 2016-11-08
 * @location peking
 * */
public class SigarTest {
	
	public static void main(String[] args) throws Exception{
		SigarTest st = new SigarTest();
	    st.sysInfo();
	    Sigar sigar = new Sigar();
	    st.memory(sigar);
	    st.cpu(sigar);
	    st.os();
	    st.filesystem(sigar);
	    st.net(sigar);
	    st.ethernet(sigar);
	}

	
	//获取系统信息（从jvm）
	public void sysInfo() throws Exception{
		
		 Runtime runtime = Runtime.getRuntime();
		 Properties props = System.getProperties();
		 Map<String, String> map = System.getenv();
	     
		 String userName = map.get("USERNAME");
		 System.out.println("用户名：" + userName);
		 
		 String computerName = map.get("COMPUTERNAME");
         System.out.println("计算机名：" + computerName);
         
         String computerDomain = map.get("USERDOMAIN");
         System.out.println("计算机域名：" + computerDomain);
         
         String ip = InetAddress.getLocalHost().getHostAddress();
         System.out.println("IP地址：" + ip);
         
         String hostName = InetAddress.getLocalHost().getHostName();
         System.out.println("主机名称：" + hostName);
         
         System.out.println("JVM总内存：" + runtime.totalMemory());
         System.out.println("JVM剩余内存：" + runtime.freeMemory());
         System.out.println("JVM可用的处理器个数：" + runtime.availableProcessors());
         System.out.println("JVM运行时版本：" + props.getProperty("java.version"));
         System.out.println("JVM运行时环境供应商：" + props.getProperty("java.vendor"));
	      
         System.out.println("操作系统名称：" + props.getProperty("os.name"));
         System.out.println("操作系统架构：" + props.getProperty("os.arch"));
         System.out.println("操作系统版本：" + props.getProperty("os.version"));
         System.out.println("用户主目录：" + props.getProperty("user.home"));
         System.out.println("用户当前工作目录：" + props.getProperty("user.dir"));
	}
	
	
	public void memory(Sigar sigar) throws Exception{
		Mem memory = sigar.getMem();
		System.out.println("-------memory------");
	    System.out.println("总内存：" + (memory.getTotal() / 1024L) + "KB");
	    System.out.println("内存使用量：" + memory.getUsed() / 1024L + "KB");
	    System.out.println("内存剩余量：" + memory.getFree() / 1024 + "KB");
	    
	    System.out.println("内存使用率：" + memory.getUsedPercent() + "%");
	    System.out.println("内存空闲率：" + memory.getFreePercent() + "%");
	    
	    Swap swap = sigar.getSwap();
	    System.out.println("交换区总量：" + swap.getTotal() / 1024L + "KB");
	    System.out.println("交换区使用量：" + swap.getUsed() / 1024L+ "KB");
	    System.out.println("交换区剩余量：" + swap.getFree() / 1024L + "KB");
	}
	
	public void cpu(Sigar sigar) throws Exception{
		CpuInfo[] cpuInfos = sigar.getCpuInfoList();
		CpuPerc[] cpuPrecs = sigar.getCpuPercList();
		System.out.println("-------cup------");
		for(int i = 0;i < cpuInfos.length;i ++){
			CpuInfo cpuInfo = cpuInfos[i];
			System.out.println("第" + (i + 1) + "块CPU");
			System.out.println("CPUMhz：" + cpuInfo.getMhz());
			System.out.println("CPU生产商：" + cpuInfo.getVendor());
			System.out.println("CPU种类：" + cpuInfo.getModel());
			System.out.println("CPU缓存大小：" + cpuInfo.getCacheSize());
			printPrecList(cpuPrecs[i]);
		}
	}
	
	private void printPrecList(CpuPerc perc){
		System.out.println("CPU用户使用率：" + CpuPerc.format(perc.getUser()));
		System.out.println("CPU系统使用率：" + CpuPerc.format(perc.getSys()));
		System.out.println("CPU当前等待率：" + CpuPerc.format(perc.getWait()));
		System.out.println("CPU当前错误率：" + CpuPerc.format(perc.getNice()));
		System.out.println("CPU当前空闲率：" + CpuPerc.format(perc.getIdle()));
		System.out.println("CPU当前总使用率：" + CpuPerc.format(perc.getCombined()));
	}
	
	
	public void os(){
		OperatingSystem os = OperatingSystem.getInstance();
		
		System.out.println("操作系统：" + os.getArch());
		System.out.println("操作系统CpuEndian():    " + os.getCpuEndian());//
        System.out.println("操作系统DataModel():    " + os.getDataModel());//
        System.out.println("操作系统描述：" + os.getDescription());
        System.out.println("操作系统提供商：" + os.getVendor());
        System.out.println("操作系统名称：" + os.getVendorName());
        System.out.println("操作系统提供商类型：" + os.getVendorVersion());
        System.out.println("操作系统版本号：" + os.getVersion());
	}
	
	
	public void filesystem(Sigar sigar){
		try{
		FileSystem[] fsList = sigar.getFileSystemList();
		for(int i = 0;i < fsList.length; i ++){
			FileSystem fs = fsList[i];
			System.out.println("盘符名称：" + fs.getDevName());
			System.out.println("盘符路径：" + fs.getDirName());
			System.out.println("盘符标记：" + fs.getFlags());
			System.out.println("盘符类型：" + fs.getSysTypeName());
			System.out.println("盘符类型名：" + fs.getTypeName());
			System.out.println("盘符文件系统类型：" + fs.getType());
			
			FileSystemUsage usage = null;
            usage = sigar.getFileSystemUsage(fs.getDirName());
            switch (fs.getType())
            {
                case 0: // TYPE_UNKNOWN ：未知
                    break;
                case 1: // TYPE_NONE
                    break;
                case 2: // TYPE_LOCAL_DISK : 本地硬盘
                    // 文件系统总大小
                    System.out.println(fs.getDevName() + "总大小:    "
                            + usage.getTotal() + "KB");
                    // 文件系统剩余大小
                    System.out.println(fs.getDevName() + "剩余大小:    "
                            + usage.getFree() + "KB");
                    // 文件系统可用大小
                    System.out.println(fs.getDevName() + "可用大小:    "
                            + usage.getAvail() + "KB");
                    // 文件系统已经使用量
                    System.out.println(fs.getDevName() + "已经使用量:    "
                            + usage.getUsed() + "KB");
                    double usePercent = usage.getUsePercent() * 100D;
                    // 文件系统资源的利用率
                    System.out.println(fs.getDevName() + "资源的利用率:    "
                            + usePercent + "%");
                    break;
                case 3:// TYPE_NETWORK ：网络
                    break;
                case 4:// TYPE_RAM_DISK ：闪存
                    break;
                case 5:// TYPE_CDROM ：光驱
                    break;
                case 6:// TYPE_SWAP ：页面交换
                    break;
            }
            System.out.println(fs.getDevName() + "读出：    "
                    + usage.getDiskReads());
            System.out.println(fs.getDevName() + "写入：    "
                    + usage.getDiskWrites());
        }
		}catch(Exception e){
			
		}
	}
	
	
	public void net(Sigar sigar) throws Exception
    {
        String ifNames[] = sigar.getNetInterfaceList();
        for (int i = 0; i < ifNames.length; i++)
        {
            String name = ifNames[i];
            NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name);
            System.out.println("网络设备名:    " + name);// 网络设备名
            System.out.println("IP地址:    " + ifconfig.getAddress());// IP地址
            System.out.println("子网掩码:    " + ifconfig.getNetmask());// 子网掩码
            if ((ifconfig.getFlags() & 1L) <= 0L)
            {
                System.out.println("!IFF_UP...skipping getNetInterfaceStat");
                continue;
            }
            NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name);
            System.out.println(name + "接收的总包裹数:" + ifstat.getRxPackets());// 接收的总包裹数
            System.out.println(name + "发送的总包裹数:" + ifstat.getTxPackets());// 发送的总包裹数
            System.out.println(name + "接收到的总字节数:" + ifstat.getRxBytes());// 接收到的总字节数
            System.out.println(name + "发送的总字节数:" + ifstat.getTxBytes());// 发送的总字节数
            System.out.println(name + "接收到的错误包数:" + ifstat.getRxErrors());// 接收到的错误包数
            System.out.println(name + "发送数据包时的错误数:" + ifstat.getTxErrors());// 发送数据包时的错误数
            System.out.println(name + "接收时丢弃的包数:" + ifstat.getRxDropped());// 接收时丢弃的包数
            System.out.println(name + "发送时丢弃的包数:" + ifstat.getTxDropped());// 发送时丢弃的包数
        }
    }
    public void ethernet(Sigar sigar) throws SigarException
    {
        String[] ifaces = sigar.getNetInterfaceList();
        for (int i = 0; i < ifaces.length; i++)
        {
            NetInterfaceConfig cfg = sigar.getNetInterfaceConfig(ifaces[i]);
            if (NetFlags.LOOPBACK_ADDRESS.equals(cfg.getAddress())
                    || (cfg.getFlags() & NetFlags.IFF_LOOPBACK) != 0
                    || NetFlags.NULL_HWADDR.equals(cfg.getHwaddr()))
            {
                continue;
            }
            System.out.println(cfg.getName() + "IP地址:" + cfg.getAddress());// IP地址
            System.out.println(cfg.getName() + "网关广播地址:" + cfg.getBroadcast());// 网关广播地址
            System.out.println(cfg.getName() + "网卡MAC地址:" + cfg.getHwaddr());// 网卡MAC地址
            System.out.println(cfg.getName() + "子网掩码:" + cfg.getNetmask());// 子网掩码
            System.out.println(cfg.getName() + "网卡描述信息:" + cfg.getDescription());// 网卡描述信息
            System.out.println(cfg.getName() + "网卡类型" + cfg.getType());//
        }
    }
}
