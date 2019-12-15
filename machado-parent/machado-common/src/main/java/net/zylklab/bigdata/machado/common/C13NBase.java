package net.zylklab.bigdata.machado.common;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.reload.ReloadStrategy;
import org.cfg4j.source.reload.strategy.PeriodicalReloadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C13NBase {
	private static final Logger _log = LoggerFactory.getLogger(C13NBase.class);
	private static Map<String,C13NBase> singletons = new HashMap<String,C13NBase>(); //para aplicaciones
	private static Map<String,HdfsConfigurationSource> sources = new HashMap<String,HdfsConfigurationSource>();
	private static Map<String,ReloadStrategy> reloadStrategys = new HashMap<String,ReloadStrategy>();
	private static Map<String,ConfigurationProvider> providers = new HashMap<String,ConfigurationProvider>();
	private static final String MACHADO_MAP_KEY = "XXX-MACHADO";
	private String key;
	private boolean reloadStrategyActive = false; 
	
	public static boolean existisProperty(ConfigurationProvider provider, String property) {
		try {
			provider.getProperty(property, String.class);
		} catch (NoSuchElementException e) {
			 return false;
		}
		return true;
	}
	
	public static C13NBase getInstance(String[] applications) {
		_log.debug("getInstance(applications)");
		String key = Arrays.toString(applications);
		if(!singletons.containsKey(key))
			singletons.put(key, new C13NBase(applications));
		_log.debug("getInstance() :: singleltons, applications "+singletons.get(key)+","+key);
		return singletons.get(key);
	}
	
	public static C13NBase getInstance() {
		_log.debug("getInstance()");
		if(!singletons.containsKey(MACHADO_MAP_KEY))
			singletons.put(MACHADO_MAP_KEY, new C13NBase());
		_log.debug("getInstance() :: singleltons, applications "+singletons.get(MACHADO_MAP_KEY)+","+MACHADO_MAP_KEY);
		return singletons.get(MACHADO_MAP_KEY);
	}
	
	private C13NBase(String[] applications) {
		_log.debug("Constructor :: C13NBase(applications)");
		String key = Arrays.toString(applications);
		this.key = key;
		if(!sources.containsKey(key)) {
			sources.put(key, new HdfsConfigurationSourceBuilder().withApplications(Arrays.asList(applications)).build());
			_log.debug("Constructor :: C13NBase() :: source, key "+sources.get(key)+","+key);
		}
		if(this.reloadStrategyActive) { 
			if(!reloadStrategys.containsKey(key)) {
			reloadStrategys.put(key, new PeriodicalReloadStrategy(5, TimeUnit.MINUTES));
			_log.debug("Constructor :: C13NBase() :: reloadStrategy, key "+reloadStrategys.get(key)+","+key);
			}
		}
		if(this.reloadStrategyActive) { 
			if(!providers.containsKey(key)) {	
				providers.put(key, new ConfigurationProviderBuilder().withConfigurationSource(sources.get(key)).withReloadStrategy(reloadStrategys.get(key)).build());
				_log.debug("Constructor with reload:: C13NBase() :: provider, applications "+providers.get(key)+","+Arrays.toString(applications));
			}
		} else {
			if(!providers.containsKey(key)) {	
				providers.put(key, new ConfigurationProviderBuilder().withConfigurationSource(sources.get(key)).build());
				_log.debug("Constructor without reload:: C13NBase() :: provider, applications "+providers.get(key)+","+Arrays.toString(applications));
			}
		}
	}
	
	private C13NBase() {
		_log.debug("Constructor :: C13NBase()");
		this.key = MACHADO_MAP_KEY;
		if(!sources.containsKey(MACHADO_MAP_KEY)) {
			sources.put(MACHADO_MAP_KEY, new HdfsConfigurationSourceBuilder().build());
			_log.debug("Constructor :: C13NBase() :: source, key "+sources.get(MACHADO_MAP_KEY)+","+MACHADO_MAP_KEY);
		}
		if(this.reloadStrategyActive) {
			if(!reloadStrategys.containsKey(MACHADO_MAP_KEY)) {
				reloadStrategys.put(MACHADO_MAP_KEY, new PeriodicalReloadStrategy(5, TimeUnit.MINUTES));
				_log.debug("Constructor :: C13NBase() :: reloadStrategy, key "+reloadStrategys.get(MACHADO_MAP_KEY)+","+MACHADO_MAP_KEY);
			}	
		}
		if(this.reloadStrategyActive) { 
			if(!providers.containsKey(MACHADO_MAP_KEY)) {	
				providers.put(MACHADO_MAP_KEY, new ConfigurationProviderBuilder().withConfigurationSource(sources.get(MACHADO_MAP_KEY)).withReloadStrategy(reloadStrategys.get(key)).build());
				_log.debug("Constructor with reload:: C13NBase() :: provider, applications "+providers.get(MACHADO_MAP_KEY)+","+MACHADO_MAP_KEY);
			}
		} else {
			if(!providers.containsKey(MACHADO_MAP_KEY)) {	
				providers.put(MACHADO_MAP_KEY, new ConfigurationProviderBuilder().withConfigurationSource(sources.get(MACHADO_MAP_KEY)).build());
				_log.debug("Constructor without reload:: C13NBase() :: provider, applications "+providers.get(MACHADO_MAP_KEY)+","+MACHADO_MAP_KEY);
			}
		}
	}
	
	public ConfigurationProvider getConfig() {
		_log.debug("getConfig() provider, key "+providers.get(this.key)+", "+this.key);
		return providers.get(this.key);
	}
	
	
	public void stopReloadStrategys() {
		_log.debug("Stopping reload thereads ");
		for (Map.Entry<String,ReloadStrategy> reloadStrategy : reloadStrategys.entrySet()) {
			_log.debug(String.format("Stopping reload thereads for the key %s",reloadStrategy.getKey()));
			reloadStrategy.getValue().deregister(sources.get(reloadStrategy.getKey()));
		} 
	}
	
	
	public static void main(String[] args) throws IOException {
		//-DHDFS_CONF_PATH=/home/gus/hadoop
		//-DLOCAL_CONF_PATH=/home/gus/hadoop/local_conf
		System.setProperty("LOCAL_CONF_PATH","/home/gus/hadoop/local_conf");
		//System.setProperty("LOCAL_CONF_PATH","/home/gus/hadoop/local_conf");
		while (true) {
			try {
				System.out.println("Leer propiedad :::::::::::::::::");
				System.in.read();
				System.out.println(C13NBase.getInstance().getConfig().getProperty("machado.hbase.zookeeper.session.timeout", String.class));
				String[] use = {"use"};
				System.out.println(C13NBase.getInstance(use).getConfig().getProperty("machado.hbase.zookeeper.session.timeout", String.class));
			} catch (java.util.NoSuchElementException e) {
				//do nothing
				
			}
		}
	}
}
