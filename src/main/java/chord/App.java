package chord;

import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class App {
    public static final String IP = System.getenv("HOST");
    public static int PORT = Integer.parseInt(System.getenv("PORT"));
    public static final Path SERVER_BASE_PATH = Paths.get("src/main/resources/" + System.getenv("PORT") + "/");
    private final ChordServer chordServer;

    public App() {
        SslContext sslContext = null;
        try {
            sslContext = ChordServer.loadTLSCredentials();
        } catch (SSLException e) {
            e.printStackTrace();
        }
//        System.out.println("sslContext: " + sslContext);
//        ChordServer server = new ChordServer(Utils.PORT, sslContext);
//        server.start();
//        server.blockUntilShutdown();
        System.out.println("Utils.PORT: " + PORT);
        System.out.println("Utils.HOST: " + IP);
//        this.chordServer = new ChordServer(PORT, sslContext);
        this.chordServer = new ChordServer(PORT);
        run();
    }

//    public App(int port) {
//        this.chordServer = new ChordServer(port);
//        run();
//    }

    public static void main(String[] args) {
//        if (args.length != 1) {
//            System.out.println("Usage: java -jar chord-1.0-SNAPSHOT.jar <port>");
//            System.exit(0);
//        }
//        int port = Integer.parseInt(args[0]);
//        App app = new App(port);
        App app = new App();
        try {
            app.run();
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    void run() {
        try {
            chordServer.start();
//            chordServer.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
