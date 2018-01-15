package com.gearsofleo.grpc.consul.discovery;

import java.net.URI;
import java.util.Optional;

import com.google.common.net.HostAndPort;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

/**
 * Name resolver provider for Consul.
 *
 * @author Andreas Lindfalk
 */
public class ConsulNameResolverProvider extends NameResolverProvider {

	private static final String CONSUL = "consul";

	private final Optional<HostAndPort> maybeConsulHostAndPort;

	/**
	 * Creates a new instance.
	 */
	public ConsulNameResolverProvider() {
		this.maybeConsulHostAndPort = Optional.empty();
	}

	/**
	 * Creates a new instance.
	 *
	 * @param consulHostAndPort Consul host and port combo, if the defaults shall not be used
	 */
	public ConsulNameResolverProvider(HostAndPort consulHostAndPort) {
		this.maybeConsulHostAndPort = Optional.of(consulHostAndPort);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean isAvailable() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected int priority() {
		return 5;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NameResolver newNameResolver(URI targetUri, Attributes params) {
		return new ConsulNameResolver(targetUri, this.maybeConsulHostAndPort);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDefaultScheme() {
		return CONSUL;
	}
}
