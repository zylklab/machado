package net.zylklab.bigdata.machado.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsConfigurationSourceBuilder {

	private static final Logger _log = LoggerFactory.getLogger(HdfsConfigurationSourceBuilder.class);
	private String host;
	private List<String> applications;
	private int port;

	private static Map<String,HdfsConfigurationSource> hdfsConfigurationSources = new HashMap<String,HdfsConfigurationSource>();

	/**
	 * Construct {@link HdfsConfigurationSource}s builder
	 * <p>
	 * Default setup (override using with*() methods)
	 * <ul>
	 * <li>host: null</li>
	 * <li>port: -1</li>
	 * <li>path: "/configuration/example.properties"</li>
	 * </ul>
	 *
	 * If you dont specify host and port, you must provide the
	 * core-site.xml and hdfs-site.xml conf files directory
	 * via HDFS_CONF_PATH en variable.
	 */
	public HdfsConfigurationSourceBuilder() {
		host = null;
		port = -1;
		applications = new ArrayList<>();
		applications.add("/configuration/machado");
		_log.debug("Constructor :: HdfsConfigurationSourceBuilder host,port,applications "+host+", "+port+", "+Arrays.toString(applications.toArray()));
	}

	/**
	 * Set Consul host for {@link HdfsConfigurationSource}s built by this
	 * builder.
	 *
	 * @param host
	 *            host to use
	 * @return this builder with Consul host set to provided parameter
	 */
	public HdfsConfigurationSourceBuilder withHost(String host) {
		this.host = host;
		_log.debug("withHost :: host,port,applications "+host+", "+port+", "+Arrays.toString(applications.toArray()));
		return this;
	}

	/**
	 * Set Consul port for {@link HdfsConfigurationSource}s built by this
	 * builder.
	 *
	 * @param port
	 *            port to use
	 * @return this builder with Consul port set to provided parameter
	 */
	public HdfsConfigurationSourceBuilder withPort(int port) {
		this.port = port;
		_log.debug("withPort :: host,port,applications "+host+", "+port+", "+Arrays.toString(applications.toArray()));
		return this;
	}

	/**
	 * Set Hdfs applications paths for {@link HdfsConfigurationSource}s built by this builder.
	 *
	 * @param applications
	 *            applications list to use
	 * @return this builder with Hdfs path set to provided parameter
	 */
	public HdfsConfigurationSourceBuilder withApplications(List<String> applications) {
		if(applications != null){
			for (String app: applications){
				if(app != null && app.length() > 0)
					this.applications.add("/configuration/app/"+app);
			}
		}
		_log.debug("withApplications :: host,port,applications "+host+", "+port+", "+Arrays.toString(applications.toArray()));
		return this;
	}

	/**
	 * Build a {@link HdfsConfigurationSource} using this builder's
	 * configuration
	 *
	 * @return new {@link HdfsConfigurationSource}
	 */
	public HdfsConfigurationSource build() {
		String key = Arrays.toString(this.applications.toArray());
		if(!hdfsConfigurationSources.containsKey(key))
			hdfsConfigurationSources.put(key, new HdfsConfigurationSource(this.host, this.port, this.applications));
		_log.debug("build :: host,port,applications "+host+", "+port+", "+Arrays.toString(applications.toArray()));
		return hdfsConfigurationSources.get(key);

	}

	@Override
	public String toString() {
		return "ConsulConfigurationSource{" + "host=" + host + ", port=" + port + '}';
	}
}
