package com.netflix.astyanax.cql;

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

/**
 * Helpful builder style class for configuring JavaDriver. 
 * 
 * @author poberai
 *
 */
public class JavaDriverConfigBuilder {

	// Config for Policies
    private LoadBalancingPolicy loadBalancingPolicy = new RoundRobinPolicy();
    private ReconnectionPolicy reconnectionPolicy = new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);
    private RetryPolicy retryPolicy = DefaultRetryPolicy.INSTANCE;

    // Config for ProtocolOptions
    private int nativeProtocolPort = -1; 
    
    // Config for PoolingOptions
    private PoolingOptions poolingOptions = new PoolingOptions();
    
    // Config for SocketOptions
    private SocketOptions socketOptions = new SocketOptions();

	// Config for NettyOptions
	private NettyOptions nettyOptions = NettyOptions.DEFAULT_INSTANCE;

	// Config for CodecRegistry
	private CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    
    // Config for MetricsOptions
    private boolean jmxReportingEnabled = true;

	// Config for MetricsOptions
	private boolean metricsEnabled = true;
    
    // Config for QueryOptions
    private QueryOptions queryOptions = new QueryOptions();
    
	public JavaDriverConfigBuilder() {
		super();
	}
	
	public JavaDriverConnectionPoolConfigurationImpl build() {

		Policies policies = Policies.builder()
				.withReconnectionPolicy(reconnectionPolicy)
				.withRetryPolicy(retryPolicy)
				.withLoadBalancingPolicy(loadBalancingPolicy)
				.build();
		
		ProtocolOptions protocolOptions = (nativeProtocolPort == -1) ? new ProtocolOptions() : new ProtocolOptions(nativeProtocolPort);
		PoolingOptions poolOptions = poolingOptions;
		SocketOptions sockOptions = socketOptions;
		MetricsOptions metricsOptions = new MetricsOptions(metricsEnabled, jmxReportingEnabled);
		QueryOptions qOptions = queryOptions;
		NettyOptions nOptions = nettyOptions;
		CodecRegistry cRegistry = codecRegistry;

		Configuration configuration = Configuration.builder()
				.withPolicies(policies)
				.withProtocolOptions(protocolOptions)
				.withPoolingOptions(poolOptions)
				.withSocketOptions(sockOptions)
				.withMetricsOptions(metricsOptions)
				.withQueryOptions(qOptions)
				.withNettyOptions(nOptions)
				.withCodecRegistry(cRegistry)
				.build();
		
		return new JavaDriverConnectionPoolConfigurationImpl(configuration);
	}
	
	public JavaDriverConfigBuilder withLoadBalancingPolicy(LoadBalancingPolicy lbPolicy) {
		this.loadBalancingPolicy = lbPolicy;
		return this;
	}
	
	public JavaDriverConfigBuilder withReconnectionPolicy(ReconnectionPolicy reconnectPolicy) {
		this.reconnectionPolicy = reconnectPolicy;
		return this;
	}
	
	public JavaDriverConfigBuilder withRetryPolicy(RetryPolicy rPolicy) {
		this.retryPolicy = rPolicy;
		return this;
	}
	
	public JavaDriverConfigBuilder withPort(int nativePort) {
		this.nativeProtocolPort = nativePort;
		return this;
	}
	
	public JavaDriverConfigBuilder withCoreConnsPerHost(HostDistance distance, int coreConnections) {
		this.poolingOptions.setCoreConnectionsPerHost(distance, coreConnections);
		return this;
	}

	public JavaDriverConfigBuilder withMaxConnsPerHost(HostDistance distance, int maxConnections) {
		this.poolingOptions.setMaxConnectionsPerHost(distance, maxConnections);
		return this;
	}

	public JavaDriverConfigBuilder withMinRequestsPerConnection(HostDistance distance, int minRequests) {
		this.poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(distance, minRequests);
		return this;
	}

	public JavaDriverConfigBuilder withMaxRequestsPerConnection(HostDistance distance, int maxRequests) {
		this.poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(distance, maxRequests);
		return this;
	}

	public JavaDriverConfigBuilder withConnectTimeout(int timeout, TimeUnit sourceUnit) {
		Long connectTimeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, sourceUnit);
		this.socketOptions.setConnectTimeoutMillis(connectTimeoutMillis.intValue());
		return this;
	}

	public JavaDriverConfigBuilder withReadTimeout(int timeout, TimeUnit sourceUnit) {
		Long readTimeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, sourceUnit);
		this.socketOptions.setReadTimeoutMillis(readTimeoutMillis.intValue());
		return this;
	}
	
	 public JavaDriverConfigBuilder withKeepAlive(boolean keepAlive) {
		 this.socketOptions.setKeepAlive(keepAlive);
		 return this;
	 }

	 public JavaDriverConfigBuilder withReuseAddress(boolean reuseAddress) {
		 this.socketOptions.setReuseAddress(reuseAddress);
		 return this;
	 }

	 public JavaDriverConfigBuilder withSoLinger(int soLinger) {
		 this.socketOptions.setSoLinger(soLinger);
		 return this;
	 }

	 public JavaDriverConfigBuilder withTcpNoDelay(boolean tcpNoDelay) {
		 this.socketOptions.setTcpNoDelay(tcpNoDelay);
		 return this;
	 }

	 public JavaDriverConfigBuilder withReceiveBufferSize(int receiveBufferSize) {
		 this.socketOptions.setReceiveBufferSize(receiveBufferSize);
		 return this;
	 }

	 public JavaDriverConfigBuilder withSendBufferSize(int sendBufferSize) {
		 this.socketOptions.setSendBufferSize(sendBufferSize);
		 return this;
	 }
	 
	 public JavaDriverConfigBuilder withJmxReportingEnabled(boolean enabled) {
		 this.jmxReportingEnabled = enabled;
		 return this;
	 }

	public JavaDriverConfigBuilder withMetricsEnabled(boolean enabled) {
		this.metricsEnabled = enabled;
		return this;
	}
	 
	public JavaDriverConfigBuilder withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.queryOptions.setConsistencyLevel(consistencyLevel);
		return this;
	}

	public JavaDriverConfigBuilder withSerialConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.queryOptions.setSerialConsistencyLevel(consistencyLevel);
		return this;
	}

	public JavaDriverConfigBuilder withFetchSize(int fetchSize) {
		this.queryOptions.setFetchSize(fetchSize);
		return this;
	}

	public JavaDriverConfigBuilder withNettyOptions(NettyOptions nettyOptions) {
		this.nettyOptions = nettyOptions;
		return this;
	}

	public JavaDriverConfigBuilder withCodecRegistry(CodecRegistry codecRegistry) {
		this.codecRegistry = codecRegistry;
		return this;
	}
}
