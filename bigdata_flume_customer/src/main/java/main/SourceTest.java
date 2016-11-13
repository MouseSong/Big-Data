package main;

import org.hyperic.sigar.Sigar;

import customer.source.SigarSource;
import customer.source.sigar.SigarMain;

public class SourceTest {

	public static void main(String[] args) throws Exception {
		/*SigarMain sm = new SigarMain(new Sigar());
		System.out.println(sm.getHostInfo());*/
		
		SigarSource ss = new SigarSource();
		ss.start();
	}
}
