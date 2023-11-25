package chord;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ChordClient {
    private static final Logger logger = Logger.getLogger(ChordClient.class.getName());

    final ManagedChannel channel;
    final ChordGrpc.ChordBlockingStub blockingStub;
    final ChordGrpc.ChordStub asyncStub;

    public ChordClient(String host, int port) {
//        channel = ManagedChannelBuilder.forAddress(host, port)
//                .usePlaintext()
//                .build();
        SslContext sslContext = null;
        try {
            sslContext = loadTLSCredentials();
        } catch (SSLException e) {
            e.printStackTrace();
        }
        channel = NettyChannelBuilder.forAddress(host, port)
                .sslContext(sslContext)
                .build();

        blockingStub = ChordGrpc.newBlockingStub(channel);
        asyncStub = ChordGrpc.newStub(channel);
    }

    public ChordClient(String host, int port, SslContext sslContext) {
        logger.info("Creating secure channel using ssl context");
        channel = NettyChannelBuilder.forAddress(host, port)
                .sslContext(sslContext)
                .build();

        blockingStub = ChordGrpc.newBlockingStub(channel);
        asyncStub = ChordGrpc.newStub(channel);
    }

    public static SslContext loadTLSCredentials() throws SSLException {
        File serverCACertFile = new File("cert/ca-cert.pem");
        File clientCertFile = new File("cert/client-cert.pem");
        File clientKeyFile = new File("cert/client-key.pem");

        return GrpcSslContexts.forClient()
                .keyManager(clientCertFile, clientKeyFile)
                .trustManager(serverCACertFile)
                .build();
    }

    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
