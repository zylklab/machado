package net.zylklab.bigdata.machado.sandbox;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.beryx.textio.TextIO;
import org.beryx.textio.TextIoFactory;
import org.beryx.textio.TextTerminal;

import net.zylklab.bigdata.machado.hbase.HBaseManager;
import net.zylklab.bigdata.machado.hbase.pool.HBasePoolException;

public class HbaseTest {

	private static final byte[] FAMILY_BASE = "b".getBytes();
	private static final byte[] QUALIFIER_FAMILY_BASE_DATE = "d".getBytes();
	private static final byte[] FAMILY_EXTRA = "e".getBytes();
	private static final byte[] QUALIFIER_FAMILY_EXTRA_TOTAL = "t".getBytes();
	private static final String TABLE = "MACHADO_TEST";
	
	private DateFormat df;
	
	public enum SINO {Y,N}
	
	public HbaseTest() {
		this.df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	}
	
	private byte[] buildKey(String partition ) {
		String key = partition + UUID.randomUUID();
		return key.getBytes();
	}
	private Put buildPut(String partition) {
		Put put =  new Put(buildKey(partition));
		put.addColumn(FAMILY_BASE, QUALIFIER_FAMILY_BASE_DATE, Bytes.toBytes(this.df.format(new Date(System.currentTimeMillis()))));
		put.addColumn(FAMILY_EXTRA, QUALIFIER_FAMILY_EXTRA_TOTAL, Bytes.toBytes(System.currentTimeMillis()));
		return put;
	}
	
	private void puts(String partition, int numberOfPuts, int msbetweenPuts) throws IOException, HBasePoolException, InterruptedException {
		for (int i = 0; i < numberOfPuts; i++) {
			Thread.sleep(msbetweenPuts);
			HBaseManager.put(buildPut(partition), TABLE);
		}
	}
	
	public static void main(String[] args) {
		System.setProperty("LOCAL_CONF_PATH", "/home/gus/workspaces/machado/machado-parent/machado-common/apps/");
		HbaseTest test = new HbaseTest();
		SINO run = SINO.Y; 
		while(run == SINO.Y) {
			TextIO textIO = TextIoFactory.getTextIO();
			String partition = textIO.newStringInputReader().withDefaultValue("0").read("In what partition (from 0-9) want to write data?");
			Integer numberOfPuts = textIO.newIntInputReader().withDefaultValue(1).read("Number of puts?");
			Integer msbetweenPuts = textIO.newIntInputReader().withDefaultValue(0).read("ms between puts?");
			TextTerminal<?> terminal = textIO.getTextTerminal();
			terminal.printf("\nWriting %d puts at Partition %s \n", numberOfPuts, partition);
			try {
				test.puts(partition,numberOfPuts, msbetweenPuts);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			run = textIO.newEnumInputReader(SINO.class).withDefaultValue(SINO.Y).read("Do you want to do another put group? (Y/N)");
		}
		System.exit(0); 
	}
	
}
