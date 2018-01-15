# grpc-consul
Utility for integrating a gRPC client with Consul.

## Usage ##

```
ManagedChannelBuilder
	.forTarget("consul://grpc-server")
	.nameResolverFactory(new ConsulNameResolverProvider())
	.loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
```