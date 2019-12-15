package net.zylklab.bigdata.machado.hbase.pool;

import java.io.IOException;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hbase.client.Connection;
import org.cfg4j.provider.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HBaseConnectionFactory extends BasePooledObjectFactory<Connection> {
	private static final Logger _log = LoggerFactory.getLogger(HBaseConnectionFactory.class);
	ConfigurationProvider configurationProvider;
	
	public HBaseConnectionFactory(ConfigurationProvider configurationProvider){
		this.configurationProvider = configurationProvider; 
	}
	
	@Override
	public Connection create() throws IOException {
		Connection c = HBaseConnectionAbstractFactory.getInstance(this.configurationProvider);
		_log.info("Hbase WRAP Connection (Create) "+c.toString());
		return c;
	}
	
	@Override
	public boolean validateObject(PooledObject<Connection> p) {
		boolean isValid = true;
		if(p.getObject() == null || p.getObject().isClosed() ||  p.getObject().isAborted()){
			isValid = false;
		}
		_log.info("Hbase WRAP Connection (validate) "+p.getObject().toString()+":"+isValid);
		return isValid;
	}
	
	@Override
	public PooledObject<Connection> wrap(Connection hbaseConnection) {
		_log.info("Hbase WRAP Connection (Wrap) "+hbaseConnection.toString());
		return new DefaultPooledObject<Connection>(hbaseConnection);
	}
	
	@Override
	public void destroyObject(PooledObject<Connection> p) throws Exception {
		_log.info("Hbase WRAP Connection (Destroy) "+p.getObject().toString());
		super.destroyObject(p);
		if(p != null && p.getObject() != null && !p.getObject().isClosed()) {
			p.getObject().close();
		}
	}

	//TODO: en el validate se podría revisar si isClosed y si isAborted

}
