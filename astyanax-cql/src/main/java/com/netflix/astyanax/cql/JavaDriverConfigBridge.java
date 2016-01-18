package com.netflix.astyanax.cql;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AuthenticationCredentials;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.cql.util.ConsistencyLevelTransform;

public class JavaDriverConfigBridge {
	
	private final AstyanaxConfiguration asConfig;
	private final ConnectionPoolConfiguration cpConfig;
	
	public JavaDriverConfigBridge(AstyanaxConfiguration asConfig, ConnectionPoolConfiguration cpConfig) {
		this.asConfig = asConfig;
		this.cpConfig = cpConfig;
	}
	
	public Configuration getJDConfig() {
		return Configuration.builder()
				.withPolicies(getPolicies())
				.withProtocolOptions(getProtocolOptions())
				.withPoolingOptions(getPoolingOptions())
				.withSocketOptions(getSocketOptions())
				.withMetricsOptions(getMetricsOptions())
				.withQueryOptions(getQueryOptions())
				.withNettyOptions(getNettyOptions())
				.withCodecRegistry(getCodecRegistry())
				.build();
	}

	private NettyOptions getNettyOptions() {
		return NettyOptions.DEFAULT_INSTANCE;
	}

	private CodecRegistry getCodecRegistry() {
		return CodecRegistry.DEFAULT_INSTANCE;
	}

	private Policies getPolicies() {
		return Policies.builder().build();
	}
	
	private LoadBalancingPolicy getLB() {
		
		switch (asConfig.getConnectionPoolType()) {
		case ROUND_ROBIN:
				return new RoundRobinPolicy();
		case TOKEN_AWARE:
				return new TokenAwarePolicy(new RoundRobinPolicy());
		case BAG:
			throw new RuntimeException("Unsupported connection pool type, use ROUND_ROBIN or TOKEN_AWARE");
		default:
			return new RoundRobinPolicy();
		}
	}
	
	private ProtocolOptions getProtocolOptions() {
			
		int port = cpConfig.getPort();
		int protocolVersion = -1; // use default
		
		AuthProvider authProvider = AuthProvider.NONE;
		
		AuthenticationCredentials creds = cpConfig.getAuthenticationCredentials();
		if (creds != null) {
			authProvider = new PlainTextAuthProvider(creds.getUsername(), creds.getPassword());
		}
		
		return new ProtocolOptions(port, protocolVersion, null, authProvider);
	}

	private PoolingOptions getPoolingOptions() {
		return new CpConfigBasedPoolingOptions();
	}
	
	private SocketOptions getSocketOptions() {
		return new CpConfigBasedSocketOptions();
	}

	private MetricsOptions getMetricsOptions() {
		return new MetricsOptions();
	}

	private QueryOptions getQueryOptions() {
		return new ConfigBasedQueryOptions();
	}

	private class CpConfigBasedPoolingOptions extends PoolingOptions {

		private CpConfigBasedPoolingOptions() {
			
		}

		@Override
		public int getCoreConnectionsPerHost(HostDistance distance) {
			return cpConfig.getMaxConnsPerHost() > 4 ? cpConfig.getMaxConnsPerHost()/2 : cpConfig.getMaxConnsPerHost();
		}

		@Override
		public int getMaxConnectionsPerHost(HostDistance distance) {
			return cpConfig.getMaxConnsPerHost();
		}
	}

	private class CpConfigBasedSocketOptions extends SocketOptions {

		private CpConfigBasedSocketOptions() {
			
		}

		@Override
		public int getConnectTimeoutMillis() {
			return cpConfig.getConnectTimeout();
		}

		@Override
		public int getReadTimeoutMillis() {
			return cpConfig.getSocketTimeout();
		}
	}
	
	private class ConfigBasedQueryOptions extends QueryOptions {

		@Override
		public ConsistencyLevel getConsistencyLevel() {
			return ConsistencyLevelTransform.getConsistencyLevel(asConfig.getDefaultReadConsistencyLevel());
		}
	}
}
