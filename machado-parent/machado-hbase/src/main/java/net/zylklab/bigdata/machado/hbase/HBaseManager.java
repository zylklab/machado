package net.zylklab.bigdata.machado.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.cfg4j.provider.ConfigurationProvider;

import net.zylklab.bigdata.machado.common.C13NBase;
import net.zylklab.bigdata.machado.hbase.pojo.ScanAndTableResult;
import net.zylklab.bigdata.machado.hbase.pool.HBaseConnectionFactory;
import net.zylklab.bigdata.machado.hbase.pool.HBasePoolException;
import net.zylklab.bigdata.machado.hbase.pool.util.HBaseConnectionUtil;





public class HBaseManager {
	
	private static GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
	private static ObjectPool<Connection> pool;
	private static HBaseConnectionUtil singleton = null;
	private static final Integer MIN_EVICTABLE_IDLE_TIME_MILLISECONDS = 3*60*60*1000;
	private static final Integer SOFT_MIN_EVICTABLE_IDLE_TIME_MILLISECONDS = 3*60*60*1000;
	private static final Integer TIME_BETWEEN_EVITION_RUN_MILLISECONDS  = 1*60*60*1000;
	
	synchronized private static HBaseConnectionUtil getInstance(ConfigurationProvider configurationProvider) throws IOException {
		if(pool == null) {
	        poolConfig.setMaxTotal(configurationProvider.getProperty("machado.hbase.pool.max.total", Integer.class));
			poolConfig.setMaxIdle(configurationProvider.getProperty("machado.hbase.pool.max.idle", Integer.class));
			poolConfig.setTestOnBorrow(configurationProvider.getProperty("machado.hbase.pool.testOnBorrow", Boolean.class));
			poolConfig.setTestOnCreate(configurationProvider.getProperty("machado.hbase.pool.testOnCreate", Boolean.class));
			poolConfig.setTestOnReturn(configurationProvider.getProperty("machado.hbase.pool.testOnReturn", Boolean.class));
			poolConfig.setTestWhileIdle(configurationProvider.getProperty("machado.hbase.pool.testWhileIdle", Boolean.class));
			if(C13NBase.existisProperty(configurationProvider,"machado.hbase.pool.minEvictableIdleTimeMillis"))
				poolConfig.setMinEvictableIdleTimeMillis(configurationProvider.getProperty("machado.hbase.pool.minEvictableIdleTimeMillis", Integer.class));
			else
				poolConfig.setMinEvictableIdleTimeMillis(MIN_EVICTABLE_IDLE_TIME_MILLISECONDS);
			
			if(C13NBase.existisProperty(configurationProvider,"machado.hbase.pool.softMinEvictableIdleTimeMillis"))
				poolConfig.setSoftMinEvictableIdleTimeMillis(configurationProvider.getProperty("machado.hbase.pool.softMinEvictableIdleTimeMillis", Integer.class));
			else
				poolConfig.setSoftMinEvictableIdleTimeMillis(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLISECONDS);
			
			if(C13NBase.existisProperty(configurationProvider,"machado.hbase.pool.timeBetweenEvictionRunsMillis"))
				poolConfig.setTimeBetweenEvictionRunsMillis(configurationProvider.getProperty("machado.hbase.pool.timeBetweenEvictionRunsMillis", Integer.class));
			else
				poolConfig.setTimeBetweenEvictionRunsMillis(TIME_BETWEEN_EVITION_RUN_MILLISECONDS);
			
			
			poolConfig.setJmxNamePrefix("HBasePool");
			pool = new GenericObjectPool<Connection>(new HBaseConnectionFactory(configurationProvider), poolConfig);
		}
		if(singleton == null) {
			singleton = new HBaseConnectionUtil(pool);
		}
		return singleton;
	}
	
	public static void put(Put put, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		HBaseManager.getInstance(config).put(put, table);
	}
	
	public static void put(Put put, String table) throws IOException, HBasePoolException {
		HBaseManager.getInstance(C13NBase.getInstance().getConfig()).put(put, table);
	}
	
	public static void put(List<Put> puts, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		HBaseManager.getInstance(config).put(puts, table);
	}
	
	public static void put(List<Put> puts, String table) throws IOException, HBasePoolException {
		HBaseManager.getInstance(C13NBase.getInstance().getConfig()).put(puts, table);
	}
	
	public static void increment(Increment increment, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		HBaseManager.getInstance(config).increment(increment, table);
	}
	
	public static void increment(Increment increment, String table) throws IOException, HBasePoolException {
		HBaseManager.getInstance(C13NBase.getInstance().getConfig()).increment(increment, table);
	}
	
	public static void increment(List<Increment> increments, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		HBaseManager.getInstance(config).increment(increments, table);
	}
	
	public static void increment(List<Increment> increments, String table) throws IOException, HBasePoolException {
		HBaseManager.getInstance(C13NBase.getInstance().getConfig()).increment(increments, table);
	}
	
	public static void delete(Delete delete, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		HBaseManager.getInstance(config).delete(delete, table);
	}
	
	public static void delete(Delete delete, String table) throws IOException, HBasePoolException {
		HBaseManager.getInstance(C13NBase.getInstance().getConfig()).delete(delete, table);
	}
	
	
	public static void delete(List<Delete> delete, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		HBaseManager.getInstance(config).delete(delete, table);
	}
	
	public static void delete(List<Delete> delete, String table) throws IOException, HBasePoolException {
		HBaseManager.getInstance(C13NBase.getInstance().getConfig()).delete(delete, table);
	}
	
	
	public static boolean exist(Get get, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(config).exist(get, table);
	}
	
	public static boolean exist(Get get, String table) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(C13NBase.getInstance().getConfig()).exist(get, table);
	}
	
	public static Result get(Get get, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(config).get(get, table);
	}
	
	public static Result get(Get get, String table) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(C13NBase.getInstance().getConfig()).get(get, table);
	}
	
	public static Result[] get(List<Get> gets, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(config).get(gets, table);
	}
	
	public static Result[] get(List<Get> gets, String table) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(C13NBase.getInstance().getConfig()).get(gets, table);
	}
	
	public static ResultScanner scan(Scan scan, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(config).scan(scan, table);
	}
	
	public static ResultScanner scan(Scan scan, String table) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(C13NBase.getInstance().getConfig()).scan(scan, table);
	}
	
	public static ScanAndTableResult scanner(Scan scan, String table, ConfigurationProvider config) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(config).scanner(scan, table);
	}
	
	public static ScanAndTableResult scanner(Scan scan, String table) throws IOException, HBasePoolException {
		return HBaseManager.getInstance(C13NBase.getInstance().getConfig()).scanner(scan, table);
	}
}
