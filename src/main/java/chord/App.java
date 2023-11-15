package chord;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class App {
    private final ChordServer chordServer;

    public App(int port) {
        this.chordServer = new ChordServer(port);
        run();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java -jar chord-1.0-SNAPSHOT.jar <port>");
            System.exit(0);
        }
        int port = Integer.parseInt(args[0]);
        App app = new App(port);
        app.run();
    }

    void run() {
        try {
            chordServer.start();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
