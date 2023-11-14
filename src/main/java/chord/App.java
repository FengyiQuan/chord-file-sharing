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
    //    private ServerSocket serverSocket;
    private ChordServer chordServer;
//    private Node client;
//    private BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(100);
//    private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 100, 5, TimeUnit.MILLISECONDS, workQueue);

    public App(int port) {
        this.chordServer = new ChordServer(port);
//        this.client =  this.chordServer.getNode();
        run();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: java -jar chord-1.0-SNAPSHOT.jar <port>");
            System.exit(0);
        }
        int port = Integer.parseInt(args[0]);
        App app = new App(port);
        app.run();
    }

    void run()  {
//        try {
//                chordServer.start();
//            } catch (IOException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        executor.execute(()-> chordServer.start());
        try {
            chordServer.start();
//            client.start();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
//        executor.execute(() -> {
//            try {
//                chordServer.start();
//            } catch (IOException | InterruptedException e) {
//                e.printStackTrace();
//            }
//        });

//        executor.execute(() -> client.start());

    }


}
