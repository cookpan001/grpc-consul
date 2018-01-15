# grpc-consul
Utility for integrating a gRPC client with Consul.

## Usage ##

```
ManagedChannelBuilder
	.forTarget("consul://<service name>")
	.nameResolverFactory(new ConsulNameResolverProvider())
	.loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
	...
```

If you don't want to use the default Consul agent host/port then use the alternative constructor and provide a HostAndPort pair.

## Notes ##

Make sure that you don't override the provided Guava version (22.0) by mistake. 
A Spring Boot 2 starter will for example downgrade to 18.0, so throw in this if required:

```
  <dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>22.0</version>
  </dependency>
```