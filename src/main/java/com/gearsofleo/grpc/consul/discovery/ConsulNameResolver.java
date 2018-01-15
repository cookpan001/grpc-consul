package com.gearsofleo.grpc.consul.discovery;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.Consul.Builder;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.cache.ConsulCache;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.health.ServiceHealth;
import com.orbitz.consul.option.QueryOptions;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import lombok.extern.slf4j.Slf4j;

/**
 * A name resolver that a gRPC message channel can use
 * to obtain healthy service instances from Consul.
 *
 * @author Andreas Lindfalk
 */
@Slf4j
public class ConsulNameResolver extends NameResolver {

	// https://github.com/OrbitzWorldwide/consul-client/issues/135
	private static final int HEALTH_CACHE_WATCH_SECONDS = 8;
	private static final long HEALTH_CACHE_AWAIT_INIT_SECONDS = 3;

	private final Consul client;

	private final URI targetUri;

	private ServiceHealthCache serviceHealthCache;

	/**
	 * Creates a new instance.
	 *
	 * @param targetUri The target uri
	 *
	 * @param maybeConsulHostAndPort Optional Consul host and port combo, if the default shall not be used
	 */
	public ConsulNameResolver(URI targetUri, Optional<HostAndPort> maybeConsulHostAndPort) {

		this.targetUri = targetUri;

		Builder clientBuilder = Consul.builder();

		if (maybeConsulHostAndPort.isPresent()) {
			clientBuilder.withHostAndPort(maybeConsulHostAndPort.get());
		}

		this.client = clientBuilder.build();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getServiceAuthority() {
		return this.targetUri.getAuthority();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start(Listener listener) {

		String serviceName = getServiceAuthority();

		HealthClient healthClient = this.client.healthClient();

		List<ServiceHealth> nodes = healthClient.getHealthyServiceInstances(serviceName).getResponse();

		updateListener(nodes, listener);

		this.serviceHealthCache = ServiceHealthCache.newCache(
				healthClient,
				serviceName,
				true,
				QueryOptions.BLANK,
				HEALTH_CACHE_WATCH_SECONDS);

		addServiceHealthCacheListener(listener);

		try {
			this.serviceHealthCache.start();

			this.serviceHealthCache.awaitInitialized(
					HEALTH_CACHE_AWAIT_INIT_SECONDS,
					TimeUnit.SECONDS);

		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Add a service health cache listener.
	 *
	 * @param listener The gRPC name resolver listener, to notify when the service health cache changes
	 */
	protected void addServiceHealthCacheListener(Listener listener) {

		this.serviceHealthCache.addListener(new ConsulCache.Listener<ServiceHealthKey, ServiceHealth>() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void notify(Map<ServiceHealthKey, ServiceHealth> map) {
				updateListener(map.values(), listener);
			}
		});
	}

	/**
	 * Update the gRPC name resolver listener.
	 *
	 * @param healthyServices Health services to inform about
	 *
	 * @param listener gRPC name resolver listener
	 */
	protected void updateListener(Collection<ServiceHealth> healthyServices, Listener listener) {

		List<EquivalentAddressGroup> addressGroups = convertToEquivalentAddressGroups(healthyServices);

		log.info("Updated gRPC name resolver listener for service: '{}' with address groups: {}",
				getServiceAuthority(), addressGroups);

		listener.onAddresses(addressGroups, Attributes.EMPTY);
	}

	/**
	 * Convert healthy services obtained from Consul to the gRPC format.
	 *
	 * @param healthyServices Health services
	 *
	 * @return List of gRPC equivalent address groups
	 */
	protected List<EquivalentAddressGroup> convertToEquivalentAddressGroups(Collection<ServiceHealth> healthyServices) {

		return healthyServices.stream().map(sh -> {

			String address = resolveAddress(sh);
			int port = sh.getService().getPort();

			return new EquivalentAddressGroup(new InetSocketAddress(address, port));

		}).collect(Collectors.toList());
	}

	/**
	 * Resolves the appropriate server address for the service given the
	 * provided health information.
	 *
	 * If a service address is provided then that will be selected as it is more
	 * specific, if not - then the node address will be picked.
	 *
	 * @param serviceHealth
	 *            service health
	 *
	 * @return resolved address which typically will be an IP address but may
	 *         also be a host name.
	 */
	protected String resolveAddress(ServiceHealth serviceHealth) {

		if (StringUtils.isNotBlank(serviceHealth.getService().getAddress())) {
			return serviceHealth.getService().getAddress();
		}

		return serviceHealth.getNode().getAddress();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		try {
			if (this.serviceHealthCache != null) {
				this.serviceHealthCache.stop();
			}

			this.client.destroy();

		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
}
