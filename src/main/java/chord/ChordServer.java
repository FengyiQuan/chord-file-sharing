package chord;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ChordServer {
    private static final Logger logger = Logger.getLogger(ChordServer.class.getName());

    //    private final int port;
    private final Server server;
    private final ChordService chordService;

    public ChordServer(int port) {
        this(ServerBuilder.forPort(port));
    }

    public ChordServer(int port,
                       SslContext sslContext) {
        this(NettyServerBuilder.forPort(port).sslContext(sslContext));
    }

    public ChordServer(ServerBuilder<?> serverBuilder) {
//        this.port = port;
        chordService = new ChordService();

        server = serverBuilder.addService(chordService)
                .addService(ProtoReflectionService.newInstance())
                .build();
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        chordService.getNode().start();
        logger.info("server started on port " + App.PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("shut down gRPC server because JVM shuts down");
            try {
                ChordServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("server shut down");
        }));
    }


    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static SslContext loadTLSCredentials() throws SSLException {
        File serverCertFile = new File("cert/server-cert.pem");
        File serverKeyFile = new File("cert/server-key.pem");
//        File clientCACertFile = new File("cert/ca-cert.pem");

        SslContextBuilder ctxBuilder = SslContextBuilder.forServer(serverCertFile, serverKeyFile)
                .clientAuth(ClientAuth.NONE);//.REQUIRE
//                .trustManager(clientCACertFile);

        return GrpcSslContexts.configure(ctxBuilder).build();
    }

//    public static void main(String[] args) throws InterruptedException, IOException {
//

//    }
}
