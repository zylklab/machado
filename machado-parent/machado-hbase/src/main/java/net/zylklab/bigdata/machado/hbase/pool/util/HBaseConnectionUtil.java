package net.zylklab.bigdata.machado.hbase.pool.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import net.zylklab.bigdata.machado.hbase.pojo.ScanAndTableResult;
import net.zylklab.bigdata.machado.hbase.pool.HBasePoolException;




public class HBaseConnectionUtil {

	private ObjectPool<Connection> pool;

	public HBaseConnectionUtil(ObjectPool<Connection> pool) {
		this.pool = pool;
	}

	public void put(Put put, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					hTable.put(put);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
	}

	public void put(List<Put> puts, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					hTable.put(puts);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
	}
	
	public void increment(Increment increment, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					hTable.increment(increment);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
	}
	
	public void increment(List<Increment> increments, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					for (Increment increment : increments) {
						hTable.increment(increment);
					}
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
	}
	
	public boolean exist(Get get, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		boolean exists = false;
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					exists = hTable.exists(get);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
		return exists;
	}
	
	public Result get(Get get, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Result r = null;
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					r = hTable.get(get);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
		return r;
	}
	
	
	public Result[] get(List<Get> gets, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Result[] r = null;
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					r = hTable.get(gets);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
		return r;
	}
	
	
	public void delete(List<Delete> delete, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					hTable.delete(delete);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
	}
	
	public void delete(Delete delete, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		
		Connection connection = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					hTable.delete(delete);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
	}	
	public ResultScanner scan(Scan scan, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Connection connection = null;
		ResultScanner scanner = null;
		try {
			connection = pool.borrowObject();
			try {
				try (Table hTable = connection.getTable(tableName)) { // autocloseable
					scanner = hTable.getScanner(scan);
				}
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
		return scanner;
	}
	
	public ScanAndTableResult scanner(Scan scan, String table) throws IOException, HBasePoolException {
		TableName tableName = TableName.valueOf(table);
		Connection connection = null;
		ResultScanner scanner = null;
		ScanAndTableResult result = null;
		try {
			connection = pool.borrowObject();
			try {
				Table hTable = connection.getTable(tableName);
				scanner = hTable.getScanner(scan);
				result = new ScanAndTableResult(scanner, hTable);
			} catch (Exception e) {
				// invalidate the object
				pool.invalidateObject(connection);
				// do not return the object to the pool twice
				connection = null;
				throw new HBasePoolException(e);
			} finally {
				// make sure the object is returned to the pool
				if (null != connection) {
					pool.returnObject(connection);
				}
			}

		} catch (Exception e) {
			throw new HBasePoolException(e);
		}
		return result;
	}
}
