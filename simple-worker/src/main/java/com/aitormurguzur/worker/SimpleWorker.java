package com.aitormurguzur.worker;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Aitor Murguzur
 */
@Path("/")
public class SimpleWorker {

    private static UndertowJaxrsServer server;

    static private String workerName;
    static private int workerPort;

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        workerName = args[0];
        workerPort = Integer.parseInt(args[1]);

        startRestServer(workerPort);
        registerInZookeeper(workerPort);

        System.out.println("Worker " + workerName + " started on port " + workerPort);
    }


    private static void startRestServer(int port) {
        System.setProperty("org.jboss.resteasy.port", String.valueOf(port));
        server = new UndertowJaxrsServer().start();
        server.deploy(RestApp.class);
    }

    private static void registerInZookeeper(int port) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("192.168.33.21:2181", new RetryNTimes(5, 1000));
        curatorFramework.start();
        ServiceInstance serviceInstance = ServiceInstance.builder()
                .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                .address("localhost")
                .port(port)
                .name("worker")
                .build();

        ServiceDiscoveryBuilder.builder(Void.class)
                .basePath("load-balancing-example")
                .client(curatorFramework)
                .thisInstance(serviceInstance)
                .build()
                .start();
    }

    @GET
    @Path("/work")
    public String work() {
        String response = "Work done by " + workerName;
        System.out.println(response);
        return response;
    }

    @ApplicationPath("/")
    public static class RestApp extends Application {

        @Override
        public Set<Class<?>> getClasses() {
            Set<Class<?>> s = new HashSet<Class<?>>();
            s.add(SimpleWorker.class);
            return s;
        }
    }
}
