package com.aitormurguzur.manager;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.jboss.resteasy.plugins.server.undertow.UndertowJaxrsServer;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Charsets.*;

/**
 * @author Aitor Murguzur
 */
@Path("/")
public class SimpleManager {

    private static UndertowJaxrsServer server;

    static private int managerPort;
    static ServiceProvider serviceProvider;

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        managerPort = Integer.parseInt(args[0]);

        startRestServer(managerPort);

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("192.168.33.21:2181", new RetryNTimes(5, 1000));
        curatorFramework.start();

        ServiceDiscovery serviceDiscovery = ServiceDiscoveryBuilder
                .builder(Void.class)
                .basePath("load-balancing-example")
                .client(curatorFramework).build();
        serviceDiscovery.start();
        serviceProvider = serviceDiscovery
                .serviceProviderBuilder()
                .serviceName("worker")
                .build();
        serviceProvider.start();

        System.out.println("Manager started on port " + managerPort);
    }

    private static void startRestServer(int port) {
        System.setProperty("org.jboss.resteasy.port", String.valueOf(port));
        server = new UndertowJaxrsServer().start();
        server.deploy(RestApp.class);
    }

    @GET
    @Path("/delegate")
    public String delegate() throws Exception {
        ServiceInstance instance = serviceProvider.getInstance();
        String address = instance.buildUriSpec();
        String response = Resources.toString(new URL(address + "/work"), UTF_8);
        System.out.println(response);

        return response;
    }

    @ApplicationPath("/")
    public static class RestApp extends Application {

        @Override
        public Set<Class<?>> getClasses() {
            Set<Class<?>> s = new HashSet<Class<?>>();
            s.add(SimpleManager.class);
            return s;
        }
    }
}
