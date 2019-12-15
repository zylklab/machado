package net.zylklab.bigdata.machado.common;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.SourceCommunicationException;
import org.cfg4j.source.context.environment.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsConfigurationSource implements ConfigurationSource {

	private static final Logger _log = LoggerFactory.getLogger(HdfsConfigurationSource.class);
	private Map<String, String> propertiesValues = null;
	private final String host;
	private final List<String> paths;
	private final int port;
	private boolean initialized;

	/**
	 * Note: use {@link HdfsConfigurationSourceBuilder} for building
	 * instances of this class.
	 * <p>
	 * Read configuration from Hdfs, located at {@code host}: {@code port} in
	 * path {@code path}.
	 *
	 * @param host
	 *            Hdfs host to connect to
	 * @param port
	 *            Hdfs port to connect to
	 * @param paths
	 *            Hdfs path list to access config
	 */
	HdfsConfigurationSource(String host, int port, List<String> paths) {
		_log.debug("Constructor ::(host,port,paths,initialized) "+host+","+port+","+paths+","+initialized);
		this.host = host;
		this.port = port;
		
		CopyOnWriteArrayList<String> p = new CopyOnWriteArrayList<String>();
		for (String s : paths) {
			p.add(s);
		}
		this.paths = p;
		initialized = false;
		
		
	}

	@Override
	public Properties getConfiguration(Environment environment) {
		_log.trace("Requesting configuration for environment: " + environment.getName());

		if (this.propertiesValues == null) {
			throw new IllegalStateException(
					"Configuration source has to be successfully initialized before you request configuration.");
		}

		Properties properties = new Properties();
		properties.putAll(propertiesValues);

		return properties;
	}

	synchronized private Map<String, String> loadProperiesFromHDFS() throws IOException {
		
		Map<String, String> propertiesValuesLocal = new HashMap<String, String>();
		_log.debug("loadProperiesFromHDFS()");
		Properties properties = new Properties();
		Configuration conf = new Configuration();
		String hdfsConfPath = System.getenv("HDFS_CONF_PATH");
		_log.debug("loadProperiesFromHDFS() :: HDFS_CONF_PATH system env "+hdfsConfPath);
		if (hdfsConfPath == null) {
			hdfsConfPath = System.getProperty("HDFS_CONF_PATH");
		}
		_log.debug("loadProperiesFromHDFS() :: HDFS_CONF_PATH system property "+hdfsConfPath);
		if (hdfsConfPath != null) {
			conf.addResource(new Path(hdfsConfPath + "/core-site.xml"));
			conf.addResource(new Path(hdfsConfPath + "/hdfs-site.xml"));
		} else {
			if (this.host == null)
				throw new IOExceptionWithCause("If you dont define HDFS_CONF_PATH env variable, you must specify host and port in builder.", null);
			conf.set("fs.defaultFS", "hdfs://" + host + ":" + port + "/");
		}
		_log.debug("loadProperiesFromHDFS() :: HDFS_CONF_PATH "+hdfsConfPath);

		try (FileSystem fs = FileSystem.get(conf)) {// autoclose

			Collections.sort(this.paths);
			Collections.reverse(this.paths);
			if (this.paths != null) {
				_log.debug("loadProperiesFromHDFS() :: paths size "+this.paths.size());
			}
			else {
				_log.warn("loadProperiesFromHDFS() :: paths is null ");
				throw new IOException("The list of paths is null, there is no one properties file to read");
			}
			
			for (String path : this.paths) {
				RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(new Path(path), false);
				while (filesIterator.hasNext()) {
					LocatedFileStatus fileStatus = filesIterator.next();
					if(fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".properties")) {
						try (FSDataInputStream fsIStream = fs.open(fileStatus.getPath())) {// autoclosable
							_log.debug("loadProperiesFromHDFS() :: Connecting to HDFS at " + path);
							properties.load(fsIStream);
							_log.debug("loadProperiesFromHDFS() :: loaded " + properties.size() + " properties");
	
							for (final String name : properties.stringPropertyNames()) {
								propertiesValuesLocal.put(name, properties.getProperty(name));
								_log.debug("loadProperiesFromHDFS() :: Load property " + name + ":" + properties.getProperty(name));
							}
						}
					}
				}
			}
		}
		return propertiesValuesLocal;
	}

	private Map<String, String> loadProperiesFromLocal() throws FileNotFoundException, IOException {
		_log.debug("loadProperiesFromLocal()");
		Map<String, String> propertiesValuesLocal = new HashMap<String, String>();
		Properties properties = new Properties();
		String localConfPath = null;
		localConfPath = System.getenv("LOCAL_CONF_PATH");
		_log.debug("loadProperiesFromLocal() :: LOCAL_CONF_PATH system env "+localConfPath);
		if (localConfPath == null) {
			localConfPath = System.getProperty("LOCAL_CONF_PATH");
		}
		_log.debug("loadProperiesFromLocal() :: LOCAL_CONF_PATH system property "+localConfPath);
		if (localConfPath == null) {
			throw new IOExceptionWithCause("You have to define HDFS_CONF_PATH env variable or LOCAL_CONF_PATH", null);
		}
		_log.debug("loadProperiesFromLocal() :: LOCAL_CONF_PATH "+localConfPath);
		Collections.sort(this.paths);
		Collections.reverse(this.paths);
		if (this.paths != null) {
			_log.debug("loadProperiesFromLocal() :: paths size "+this.paths.size());
		}
		else {
			_log.warn("loadProperiesFromLocal() :: paths is null ");
			throw new IOException("The list of paths is null, there is no one properties file to read");
		}
		for (String path : this.paths) {

			File directory = new File(localConfPath + path);
			if( !directory.exists()) {
				_log.warn("loadProperiesFromLocal() :: paths is null ");
				throw new IOException("The local directory do not exists "+localConfPath + path);
			}
			// get all the files from a directory that has properties extension
			File[] fList = directory.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if (pathname.getName().endsWith(".properties") && pathname.isFile())
						return true;
					else
						return false;
				}
			});
			for (File file : fList) {
				_log.debug("loadProperiesFromLocal() ::  Connecting to Local FS at " + path);
				properties.load(new FileInputStream(file));
				_log.debug("loadProperiesFromLocal() ::  loaded " + properties.size() + " properties");
				for (final String name : properties.stringPropertyNames()) {
					propertiesValuesLocal.put(name, properties.getProperty(name));
					_log.debug("loadProperiesFromLocal() ::  Load property " + name + ":" + properties.getProperty(name));

				}
			}
		}
		return propertiesValuesLocal;
	}

	
	private void setPropertiesValues(Map<String, String> propertiesValues) {
		this.propertiesValues = propertiesValues;
	}
	
	/**
	 * .printStackTrace()
	 * 
	 * @throws SourceCommunicationException
	 *             when unable to connect to Consul client
	 */
	@Override
	public void init() {
		_log.debug("init() ::");
		try {
			_log.debug("init() :: initialized "+initialized);
			if (initialized == false) {
				String localConfPath = null;
				localConfPath = System.getenv("LOCAL_CONF_PATH");
				if (localConfPath == null) {
					localConfPath = System.getProperty("LOCAL_CONF_PATH");
				}
				String hdfsConfPath = null;
				hdfsConfPath = System.getenv("HDFS_CONF_PATH");
				if (hdfsConfPath == null) {
					hdfsConfPath = System.getProperty("HDFS_CONF_PATH");
				}
				_log.debug("init() :: localConfPath, hdfsConfPath "+localConfPath+", "+hdfsConfPath);
				Map<String, String> propertiesValuesLocal = null;
				if (localConfPath != null && hdfsConfPath == null) { // si se ha especificado configuración local y no se ha especificado configuración remota..
					_log.debug("init() :: load from local ");
					propertiesValuesLocal = loadProperiesFromLocal();
				} else {
					_log.debug("init() :: load from hdfs ");
					propertiesValuesLocal = loadProperiesFromHDFS();
				}
				this.setPropertiesValues(propertiesValuesLocal);
			}

		} catch (IOException e) {
			e.printStackTrace();
			_log.error("Error loading properties ", e);
		}
	}

	@Override
	public void reload() {
		_log.debug("Reloading configuration");
		initialized = false;
		this.init();
	}

	@Override
	public String toString() {
		return "HdfsConfigurationSource{" + "hdfsValues=" + propertiesValues + '}';
	}	 
}
