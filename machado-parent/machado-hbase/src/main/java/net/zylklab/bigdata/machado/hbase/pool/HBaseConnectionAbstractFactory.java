package net.zylklab.bigdata.machado.hbase.pool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.cfg4j.provider.ConfigurationProvider;

import net.zylklab.bigdata.machado.common.C13NBase;


public class HBaseConnectionAbstractFactory {
	
	private static Configuration conf = null;
	public synchronized static  Connection getInstance(ConfigurationProvider configurationProvider) throws IOException {
		if(conf == null) {
			conf = HBaseConfiguration.create();
			//conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", configurationProvider.getProperty("machado.hbase.zookeeper.quorum", String.class));
			conf.set("hbase.zookeeper.property.clientPort",configurationProvider.getProperty("machado.hbase.zookeeper.property.clientPort", String.class));
			conf.set("zookeeper.recovery.retry",configurationProvider.getProperty("machado.hbase.zookeeper.recovery.retry", String.class));
			conf.set("zookeeper.session.timeout",configurationProvider.getProperty("machado.hbase.zookeeper.session.timeout", String.class));
			conf.set("hbase.client.retries.number",configurationProvider.getProperty("machado.hbase.client.retries.number", String.class));
			if(C13NBase.existisProperty(configurationProvider,"machado.hbase.zookeeper.znode.parent"))
				conf.set("zookeeper.znode.parent", configurationProvider.getProperty("machado.hbase.zookeeper.znode.parent", String.class));
			if(C13NBase.existisProperty(configurationProvider,"machado.hbase.client.keyvalue.maxsize")) //10485760 10MB
				conf.set("hbase.client.keyvalue.maxsize", configurationProvider.getProperty("machado.hbase.client.keyvalue.maxsize", String.class));
		}
		Connection c = null;
		try {
			c = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return c;
	} 
}
