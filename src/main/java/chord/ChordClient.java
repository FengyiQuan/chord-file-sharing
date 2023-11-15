package chord;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ChordClient {
    private static final Logger logger = Logger.getLogger(ChordClient.class.getName());

    final ManagedChannel channel;
    final ChordGrpc.ChordBlockingStub blockingStub;
    final ChordGrpc.ChordStub asyncStub;

    public ChordClient(String host, int port) {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
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

    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
