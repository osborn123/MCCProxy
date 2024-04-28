package org.mccproxy.service;


import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * A sample gRPC server that serve the RouteGuide (see route_guide.proto) service.
 */
public class MCCProxyServer {
    private static final Logger logger = LoggerFactory.getLogger(MCCProxyServer.class.getName());

    private final int port;
    private final Server server;

    /**
     * Create a RouteGuide server listening on {@code port}.
     */
    public MCCProxyServer(int port) throws IOException {
        this(Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()),
                port);
    }

    public MCCProxyServer(ServerBuilder<?> serverBuilder, int port) throws IOException {
        this.port = port;
        server = serverBuilder.addService(new MCCProxyService())
                .build();
    }

    /**
     * Main method.  This comment makes the linter happy.
     */
    public static void main(String[] args) throws Exception {
        MCCProxyServer server = new MCCProxyServer(8980);
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    MCCProxyServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    /**
     * Stop serving requests and shutdown resources.
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Our implementation of RouteGuide service.
     *
     * <p>See route_guide.proto for details of the methods.
     */
    private static class MCCProxyService extends MCCProxyServiceGrpc.MCCProxyServiceImplBase {

        MCCProxyService() {
        }

        /**
         * @param request          the requested location for the feature.
         * @param responseObserver the observer that will receive the feature at the requested point.
         */
        @Override
        public void read(ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }

        /**
         * @param request          the bounding rectangle for the requested features.
         * @param responseObserver the observer that will receive the features.
         */
        @Override
        public void invalidate(InvalidateRequest request, StreamObserver<InvalidateResponse> responseObserver) {
            responseObserver.onNext(null);
            responseObserver.onCompleted();
        }


    }
}