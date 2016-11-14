package customer.bean;

import java.util.Arrays;

/**
 * 主机信息：CPU，硬盘，内存，操作系统.
 * @author zhangdong
 * */
public class HostInfo {

	private String cpuNum;   //cpu个数
	private Cpu[] cpus;
	
	private String diskNum;  //磁盘块数
	private FS[] fileSystem; 
	
	private Memory memory;   //内存
	private OS os;           //操作系统
	
	
	public String getCpuNum() {
		return cpuNum;
	}


	public void setCpuNum(String cpuNum) {
		this.cpuNum = cpuNum;
	}


	public Cpu[] getCpus() {
		return cpus;
	}


	public void setCpus(Cpu[] cpus) {
		this.cpus = cpus;
	}


	public String getDiskNum() {
		return diskNum;
	}


	public void setDiskNum(String diskNum) {
		this.diskNum = diskNum;
	}


	public FS[] getFileSystem() {
		return fileSystem;
	}


	public void setFileSystem(FS[] fileSystem) {
		this.fileSystem = fileSystem;
	}


	public Memory getMemory() {
		return memory;
	}


	public void setMemory(Memory memory) {
		this.memory = memory;
	}


	public OS getOs() {
		return os;
	}


	public void setOs(OS os) {
		this.os = os;
	}


	public static class OS{
		private static OS OS = new OS();
		private OS(){}
		private String osName;   //操作系统名称
		private String osDesc;   //操作系统描述
		private String osVender; //操作系统提供商
		private String osVenderName; //提供商名称
		private String osVersion;   //版本
		public String getOsName() {
			return osName;
		}
		public void setOsName(String osName) {
			this.osName = osName;
		}
		public String getOsDesc() {
			return osDesc;
		}
		public void setOsDesc(String osDesc) {
			this.osDesc = osDesc;
		}
		public String getOsVender() {
			return osVender;
		}
		public void setOsVender(String osVender) {
			this.osVender = osVender;
		}
		public String getOsVenderName() {
			return osVenderName;
		}
		public void setOsVenderName(String osVenderName) {
			this.osVenderName = osVenderName;
		}
		public String getOsVersion() {
			return osVersion;
		}
		public void setOsVersion(String osVersion) {
			this.osVersion = osVersion;
		}
		
		public static OS getOS(){
			return OS;
		}
		@Override
		public String toString() {
			return "OS [osName=" + osName + ", osDesc=" + osDesc + ", osVender=" + osVender + ", osVenderName="
					+ osVenderName + ", osVersion=" + osVersion + "]";
		}
		
		
	}
	
	//memory
	public static class Memory{
		public static Memory MEMOEY = new Memory();
		private Memory(){}
		private String memoryTotal;      //总容量
		private String memoryUsed;       //使用量
		private String memoryFree;       //剩余量
		private String memoryUsedPercent;//使用率
		private String memoryFreePercent;//剩余率
		private String swapTotal;        //交换区总容量
		private String swapUsed;         //交换区使用量
		private String swapFree;         //交换区剩余量
		public String getMemoryTotal() {
			return memoryTotal;
		}
		public void setMemoryTotal(String memoryTotal) {
			this.memoryTotal = memoryTotal;
		}
		public String getMemoryUsed() {
			return memoryUsed;
		}
		public void setMemoryUsed(String memoryUsed) {
			this.memoryUsed = memoryUsed;
		}
		public String getMemoryFree() {
			return memoryFree;
		}
		public void setMemoryFree(String memoryFree) {
			this.memoryFree = memoryFree;
		}
		public String getMemoryUsedPercent() {
			return memoryUsedPercent;
		}
		public void setMemoryUsedPercent(String memoryUsedPercent) {
			this.memoryUsedPercent = memoryUsedPercent;
		}
		public String getMemoryFreePercent() {
			return memoryFreePercent;
		}
		public void setMemoryFreePercent(String memoryFreePercent) {
			this.memoryFreePercent = memoryFreePercent;
		}
		public String getSwapTotal() {
			return swapTotal;
		}
		public void setSwapTotal(String swapTotal) {
			this.swapTotal = swapTotal;
		}
		public String getSwapUsed() {
			return swapUsed;
		}
		public void setSwapUsed(String swapUsed) {
			this.swapUsed = swapUsed;
		}
		public String getSwapFree() {
			return swapFree;
		}
		public void setSwapFree(String swapFree) {
			this.swapFree = swapFree;
		}
		
		public static Memory getMem(){
			return new Memory();
		}
		@Override
		public String toString() {
			return "Memory [memoryTotal=" + memoryTotal + ", memoryUsed=" + memoryUsed + ", memoryFree=" + memoryFree
					+ ", memoryUsedPercent=" + memoryUsedPercent + ", memoryFreePercent=" + memoryFreePercent
					+ ", swapTotal=" + swapTotal + ", swapUsed=" + swapUsed + ", swapFree=" + swapFree + "]";
		}
		
	}
	
	//dis
	public static class FS{
		 public static FS FS = new FS();
		 private FS(){}
	     private String devName;  //盘符名称
	     private String dirName;  //盘符路径
	     private String flag;     //盘符标记
	     private String type;     //磁盘类型
	     private String typeName; //类型名称
	     private String fsType;   //文件系统类型
		public String getDevName() {
			return devName;
		}
		public void setDevName(String devName) {
			this.devName = devName;
		}
		public String getDirName() {
			return dirName;
		}
		public void setDirName(String dirName) {
			this.dirName = dirName;
		}
		public String getFlag() {
			return flag;
		}
		public void setFlag(String flag) {
			this.flag = flag;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public String getTypeName() {
			return typeName;
		}
		public void setTypeName(String typeName) {
			this.typeName = typeName;
		}
		public String getFsType() {
			return fsType;
		}
		public void setFsType(String fsType) {
			this.fsType = fsType;
		}
		
		public static FS[] getFS(int size){
			FS[] fss = new FS[size];
			for(int i = 0;i < size; i ++){
				fss[i] = new FS();
			}
			return fss;
		}
		@Override
		public String toString() {
			return "FS [devName=" + devName + ", dirName=" + dirName + ", flag=" + flag + ", type=" + type
					+ ", typeName=" + typeName + ", fsType=" + fsType + "]";
		}
		
	}
	
	
	//cpu
	public static class Cpu{
		public static Cpu CPU = new Cpu();
		private Cpu(){}
		private String cpuNo;   //编号
		private String mhz;     //主频
		private String vendor;  //制造商
		private String model;   //类型
		private String cacheSize;//缓存大小
		public String getCpuNo() {
			return cpuNo;
		}
		public void setCpuNo(String cpuNo) {
			this.cpuNo = cpuNo;
		}
		public String getMhz() {
			return mhz;
		}
		public void setMhz(String mhz) {
			this.mhz = mhz;
		}
		public String getVendor() {
			return vendor;
		}
		public void setVendor(String vendor) {
			this.vendor = vendor;
		}
		public String getModel() {
			return model;
		}
		public void setModel(String model) {
			this.model = model;
		}
		public String getCacheSize() {
			return cacheSize;
		}
		public void setCacheSize(String cacheSize) {
			this.cacheSize = cacheSize;
		}
		public static Cpu[] getCpu(int size){
			Cpu[] cpus = new Cpu[size];
			for(int i = 0 ;i < size;i ++){
				cpus[i] = new Cpu();
			}
			return cpus;
		}
		@Override
		public String toString() {
			return "Cpu [cpuNo=" + cpuNo + ", mhz=" + mhz + ", vendor=" + vendor + ", model=" + model + ", cacheSize="
					+ cacheSize + "]";
		}
	}

	
	
}
