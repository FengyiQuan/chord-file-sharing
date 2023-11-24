package chord;

import java.io.IOException;

public class App {
    private final ChordServer chordServer;

    public App() {
        //        SslContext sslContext = ChordServer.loadTLSCredentials();
//        ChordServer server = new ChordServer(Utils.PORT, sslContext);
//        server.start();
//        server.blockUntilShutdown();
        this.chordServer = new ChordServer(Utils.PORT);
        run();
    }

//    public App(int port) {
//        this.chordServer = new ChordServer(port);
//        run();
//    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -jar chord-1.0-SNAPSHOT.jar <port>");
            System.exit(0);
        }
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
            chordServer.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
