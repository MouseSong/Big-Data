package customer.source.sigar;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;

import customer.bean.HostInfo;

/**
 * 收集系统信息，并返回Bean对象.
 * @author zhangdong
 * @createtime 2016-11-13
 * @location peking
 * */
public class SigarMain {

	private Sigar sigar;
	
	
	public SigarMain(Sigar sigar){
		this.sigar = sigar;
	}
	
	public HostInfo getHostInfo() throws Exception{
		HostInfo hostInfo = new HostInfo();
		fit(hostInfo);
		return hostInfo;
	}
	
	private void fit(HostInfo hostInfo){
        HostInfo.OS os = fitOS();
        hostInfo.setOs(os);
        
        HostInfo.Cpu[] cpus = fitCpu();
        hostInfo.setCpuNum(String.valueOf(cpus.length));
        hostInfo.setCpus(cpus);
        
        HostInfo.FS[] fss = fitFS();
        hostInfo.setDiskNum(String.valueOf(fss.length));
        hostInfo.setFileSystem(fss);
        
        HostInfo.Memory memory = fitMemory();
        hostInfo.setMemory(memory);
	}
	
	private HostInfo.Memory fitMemory(){
		
		HostInfo.Memory memory = null;
		try{
			memory = HostInfo.Memory.getMem();
			Mem mem = sigar.getMem();
			
			memory.setMemoryTotal(String.valueOf(mem.getTotal()));
			memory.setMemoryUsed(String.valueOf(mem.getUsed()));
			memory.setMemoryFree(String.valueOf(mem.getFree()));
			memory.setMemoryUsedPercent(String.valueOf(mem.getUsedPercent()));
			memory.setMemoryFreePercent(String.valueOf(mem.getFreePercent()));
			
			Swap swap = sigar.getSwap();
			memory.setSwapTotal(String.valueOf(swap.getTotal()));
			memory.setSwapFree(String.valueOf(swap.getFree()));
			memory.setSwapUsed(String.valueOf(swap.getUsed()));
			
		}catch(SigarException e){
            e.printStackTrace();			
		}
		return memory;
	}
	
	private HostInfo.FS[] fitFS(){
		HostInfo.FS[] fss = null;
		FileSystem[] fsList = null;
		try{
			fsList = sigar.getFileSystemList();
			fss = HostInfo.FS.getFS(fsList.length);
			for(int i = 0;i < fsList.length; i ++){
				FileSystem fs = fsList[i];
				fss[i].setDevName(fs.getDevName());
				fss[i].setDirName(fs.getDirName());
				fss[i].setFlag(String.valueOf(fs.getFlags()));
				fss[i].setFsType(String.valueOf(fs.getType()));
				fss[i].setType(String.valueOf(fs.getSysTypeName()));
				fss[i].setTypeName(fs.getTypeName());
			}
		}catch(SigarException e){
			e.printStackTrace();
		}
		return fss;
	}
	
	
	private HostInfo.Cpu[] fitCpu(){
		HostInfo.Cpu[] cpu = null;
		try{
			CpuInfo[] cpuInfos = sigar.getCpuInfoList();
			cpu = HostInfo.Cpu.getCpu(cpuInfos.length);
			System.out.println(cpu.length);
			for(int i = 0;i < cpuInfos.length;i ++){
				CpuInfo cpuInfo = cpuInfos[i];
				cpu[i].setMhz(String.valueOf(cpuInfo.getMhz()));
				cpu[i].setVendor(cpuInfo.getVendor());
				cpu[i].setModel(cpuInfo.getModel());
				cpu[i].setCacheSize(String.valueOf(cpuInfo.getCacheSize()));
			}
		}catch(SigarException e){
			e.printStackTrace();
		}
		return cpu;
	}
	
	
	private HostInfo.OS fitOS(){
		HostInfo.OS os = HostInfo.OS.getOS();
        OperatingSystem system = OperatingSystem.getInstance();
        os.setOsName(system.getArch());
        os.setOsDesc(system.getDescription());
        os.setOsVender(system.getVendor());
        os.setOsVenderName(system.getVendorName());
        os.setOsVersion(system.getVersion());
        return os;
	}
	
}
