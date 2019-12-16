package net.zylklab.bigdata.machado.sandbox;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.beryx.textio.TextIO;
import org.beryx.textio.TextIoFactory;
import org.beryx.textio.TextTerminal;

import net.zylklab.bigdata.machado.hbase.HBaseManager;

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
	private Put buildPut(byte[] key) {
		Put put =  new Put(key);
		put.addColumn(FAMILY_BASE, QUALIFIER_FAMILY_BASE_DATE, Bytes.toBytes(this.df.format(new Date(System.currentTimeMillis()))));
		put.addColumn(FAMILY_EXTRA, QUALIFIER_FAMILY_EXTRA_TOTAL, Bytes.toBytes(System.currentTimeMillis()));
		return put;
	}
	
	private Get buildGet(byte[] key) {
		Get get =  new Get(key);
		get.addColumn(FAMILY_BASE, QUALIFIER_FAMILY_BASE_DATE);
		get.addColumn(FAMILY_EXTRA, QUALIFIER_FAMILY_EXTRA_TOTAL);
		return get;
	}
	
	private void puts(String partition, int numberOfPuts, int msbetweenPuts) {
		for (int i = 0; i < numberOfPuts; i++) {
			try {
				Thread.sleep(msbetweenPuts);
				byte[] key = buildKey(partition);
				HBaseManager.put(buildPut(key), TABLE);
				Result result = HBaseManager.get(buildGet(key), TABLE);
				String date = Bytes.toString(result.getValue(FAMILY_BASE, QUALIFIER_FAMILY_BASE_DATE));
				System.out.println(date);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		System.setProperty("LOCAL_CONF_PATH", "/home/gus/workspaces/machado/machado-parent/machado-common/apps/");
		HbaseTest test = new HbaseTest();
		SINO run = SINO.Y; 
		while(run == SINO.Y) {
			TextIO textIO = TextIoFactory.getTextIO();
			String partition = textIO.newStringInputReader().withDefaultValue("0").read("In what partition (from 0-9) want to write data?");
			Integer numberOfPuts = textIO.newIntInputReader().withDefaultValue(1).read("Number of puts & gets?");
			Integer msbetweenPuts = textIO.newIntInputReader().withDefaultValue(0).read("ms between puts & gets?");
			TextTerminal<?> terminal = textIO.getTextTerminal();
			terminal.printf("\nWriting %d puts at Partition %s \n", numberOfPuts, partition);
			test.puts(partition,numberOfPuts, msbetweenPuts);
			run = textIO.newEnumInputReader(SINO.class).withDefaultValue(SINO.Y).read("Do you want to do another put group? (Y/N)");
		}
		System.exit(0); 
	}
	
}
