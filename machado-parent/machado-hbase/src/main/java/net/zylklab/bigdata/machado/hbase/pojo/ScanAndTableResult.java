package net.zylklab.bigdata.machado.hbase.pojo;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

public class ScanAndTableResult implements AutoCloseable {

	private ResultScanner scanner;
	private Table table;
	
	public ScanAndTableResult(ResultScanner scanner, Table table) {
		this.scanner = scanner;
		this.table = table;
	}
	public ResultScanner getScanner() {
		return scanner;
	}
	public Table getTable() {
		return table;
	}
	@Override
	public void close() throws Exception {
		this.scanner.close();
		this.table.close();
	}
}
