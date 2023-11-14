package chord;


import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
//import observer.FileRequestObserver;
//import observer.FileUploadObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Node extends ChordGrpc.ChordImplBase {


    private static final Logger logger = Logger.getLogger(Node.class.getName());
    public static final int M = 2; // bits of hash
    public static final int MAX_NUMBER_NODE = (int) Math.pow(2, M); // 2^M
//    private ServerSocket serverSocket;
//    private BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(100);
//    private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 100, 5, TimeUnit.MILLISECONDS, workQueue);

    private final String ip;
    private final int port;
    private final int id;

    // key is 1,2,4,8,16
    private Map<Integer, FingerInfo> fingerTable;
    private final List<Integer> fingerStart; // (n+2^(k-1) % 2^m), 1 <= k <= m
    private final Map<Long, String> fileMap; //存储其负责的标识符，String is key
    private FingerInfo predecessor; //前驱
//    private FingerInfo successor; //后继

    public Node(int port) {
        this("localhost", port);
    }

    public Node(String ip, int port) {
        System.out.println("running on port:" + port);
        this.ip = ip;
        this.port = port;
        this.id = Utils.getKey(ip, port);
//        System.out.println("id:" + this.id);
        this.fingerTable = new HashMap<>();
        this.fingerStart = new ArrayList<>();
        this.fileMap = new HashMap<>();
        for (int k = 0; k <= M; k++) {
            this.fingerStart.add((int) ((this.id + Math.pow(2, k)) % MAX_NUMBER_NODE));
        }
//        this.id = Utils.getKey(ip, port);
//        this.keyList = new HashMap<>(); // hash, file name
//        this.fingerTable = new HashMap<>();
//        for (int i = 0; i <= M; i++) {
//            this.fingerTable.put(i, new FingerInfo(this.ip, this.port, this.id));
//        }
//        this.predecessor = new FingerInfo(this.ip, this.port, this.id);
//        this.successor = new FingerInfo(this.ip, this.port, this.id);
    }

    public void start() {
        userInputHandler();
    }

    private void userInputHandler() {

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter text. Type 'exit' to quit.");
        while (true) {
            System.out.print("p2p_bash>");
            String input = scanner.nextLine();

            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Exiting program.");
                System.exit(0);
            }

            String[] inputArray = input.split("\\s+");
            String command = inputArray[0];
            String[] commandArgs = new String[inputArray.length - 1];
            for (int i = 1; i < inputArray.length; i++) {
                commandArgs[i - 1] = inputArray[i];
            }
            System.out.println("Command: " + command);
            System.out.println("Args: " + String.join(" ", commandArgs));
            if (command.equalsIgnoreCase("send")) {
                try {
                    System.out.println("Sending file...");
//                    this.sendFile(inputArray[1], inputArray[2], Integer.parseInt(inputArray[3]));
                    this.sendFile(inputArray[1]);
//                    Socket socket = new Socket("localhost", Integer.parseInt(inputArray[2]));
////                        MessageSender.sendFile(socket, inputArray[1]);
//                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (command.equalsIgnoreCase("join")) {
                System.out.println("Join Chord ring...");
                if (inputArray.length == 1)
                    this.join();
                else {

                    this.join(inputArray[1], Integer.parseInt(inputArray[2]));
                    System.out.println("Joined Chord ring.");
                }


            } else if (command.equalsIgnoreCase("ftable")) {
                this.printFTable();
            } else if (command.equalsIgnoreCase("printFileMap")) {
                this.printFileMap();
            } else if (command.equalsIgnoreCase("downloadFile")) {
                this.downloadFile(inputArray[1]);
//                ChordClient chordClient = new ChordClient(inputArray[2], Integer.parseInt(inputArray[3]));
//                try {
//                    FingerInfo successor = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(this.id).build());
//                    System.out.println(successor.toString());
//                } catch (StatusRuntimeException e) {
//                    logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
//                    return;
//                }


            }

        }
    }

    // 都是issue rpc 的, client端
    // first join the ring
    public void join() {
        for (int i = 0; i < M; i++) {
            this.fingerTable.put(i, FingerInfo.newBuilder().setIp(this.ip).setPort(this.port).setId(this.id).build());
        }
        this.predecessor = FingerInfo.newBuilder().setIp(this.ip).setPort(this.port).setId(this.id).build();
        // TODO: fix fingers
    }


    public void join(String remoteAddress, int remotePort) {
        ChordClient chordClient = new ChordClient(remoteAddress, remotePort);
//        System.out.println(chordClient);
        // check if remote address is valid
        this.initFingerTable(remoteAddress, remotePort);
        this.updateOthers();
        this.copyKeys();
        // TODO: fix fingers
    }

    //    initialize finger table of  local node
    public void initFingerTable(String remoteAddress, int remotePort) {
        System.out.println("remoteAddress: " + remoteAddress + " remotePort: " + remotePort);
        ChordClient chordClient = new ChordClient(remoteAddress, remotePort);
        FingerInfo successor = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(this.id).build());
        this.fingerTable.put(0, successor);
        System.out.println("successor: " + successor.toString());
        ChordClient successorClient = new ChordClient(this.getSuccessor().getIp(), this.getSuccessor().getPort());
        FingerInfo predecessor = successorClient.blockingStub.getPredecessor(GetPredecessorRequest.newBuilder().build());

        this.predecessor = predecessor;
        ChordClient predecessorClient = new ChordClient(predecessor.getIp(), predecessor.getPort());
        ResponseStatus pStatus = predecessorClient.blockingStub.setSuccessor(this.getSelfFingerInfo());
        ResponseStatus sStatus = successorClient.blockingStub.setPredecessor(this.getSelfFingerInfo());
//        successorClient.shutdown();
//        predecessorClient.shutdown();
        logger.info("initFingerTable result: pStatus" + pStatus.getStatus() + ", sStatus" + sStatus.getStatus());
        for (int i = 0; i < M; i++) {
//            if (Utils.inside(this.fingerStart.get(i + 1), this.id, this.fingerTable.get(i).getId(), true, false)) {
//                this.fingerTable.put(i + 1, this.fingerTable.get(i));
//            } else {
            FingerInfo newFinger = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(this.fingerStart.get(i + 1)).build());
//            chordClient.shutdown();
            this.fingerTable.put(i + 1, newFinger);
//            }
        }
    }

    // update all nodes whose finger tables should refer to this node
    private void updateOthers() {
        System.out.println("updateOthers called");
        this.printFTable();
        for (int i = 0; i < M; i++) {
            // find last node p whose i_th finger might be this node
            FingerInfo p = this.findPredecessor(this.id - (int) Math.pow(2, i));
            System.out.println(p.toString());
            ChordClient chordClient = new ChordClient(p.getIp(), p.getPort());
//            System.out.println("calling updateFingerTable");
            ResponseStatus response = chordClient.blockingStub.updateFingerTable(UpdateFingerRequest.newBuilder().setFinger(this.getSelfFingerInfo()).setIndex(i).build());
//            System.out.println("called updateFingerTable end");
//            chordClient.shutdown();

        }
//        System.out.println("updateOthers finished");
    }

    public List<String> getKeys(long id) {
        List<String> keys = new ArrayList<>();
        for (Map.Entry<Long, String> entry : this.fileMap.entrySet()) {
            if (Utils.inside(entry.getKey(), this.predecessor.getId(), id, false, true)) {
                keys.add(entry.getValue());
            }
        }
        return keys;
    }

    public void removeKey(long id) {
        this.fileMap.remove(id);
    }

    public void addKey(long id, String fileName) {
        this.fileMap.put(id, fileName);
    }

    public int getId() {
        return id;
    }

    public Map<Integer, FingerInfo> getFingerTable() {
        return fingerTable;
    }

    public void setFingerEntry(int index, FingerInfo fingerInfo) {
        this.fingerTable.put(index, fingerInfo);
    }


    public String getFileName(long key) {
        return this.fileMap.get(key);
    }

    public FingerInfo getSuccessor() {
        return this.fingerTable.get(0);
    }

    public void setSuccessor(FingerInfo successor) {
        this.fingerTable.put(0, successor);
    }

    public FingerInfo getPredecessor() {
        return this.predecessor;
    }

    public void setPredecessor(FingerInfo predecessor) {
        this.predecessor = predecessor;
    }

    public void copyKeys() {
        ChordClient chordClient = new ChordClient(this.getSuccessor().getIp(), this.getSuccessor().getPort());
        Iterator<FileRequest> fileChunks;
        try {
            fileChunks = chordClient.blockingStub.downloadFile(TargetId.newBuilder().setId(this.id).build());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        OutputStream writer = null;

        while (fileChunks.hasNext()) {
            try {
                FileRequest chunk = fileChunks.next();
                if (chunk.hasMetadata() && writer == null) {
                    writer = Utils.getFilePath(chunk);
                } else if (chunk.hasMetadata()) {
                    Utils.closeFile(writer);
                    writer = Utils.getFilePath(chunk);

                } else {
                    if (writer == null) {
                        throw new IOException("writer not properly initialized");
                    }
                    Utils.writeFile(writer, chunk.getFile().getContent());
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error writing file: " + e.getMessage());
            }
        }
        Utils.closeFile(writer);
    }

//    private long lookupResponsibleNode(long id) {
//        // If keyId at this node, no need to make rpc call
//        if (id == this.id) {
//            return this.id;
//        } else if (id == this.getSuccessor().getId()) {
//            // If only one node
//            return this.id;
//        }else if (){
//
//        }
//    }

    // TODO: download address should choose from finger table
    public void downloadFile(String file) {
        FingerInfo successor = this.getSuccessor();
        ChordClient chordClient = new ChordClient(successor.getIp(), successor.getPort());
        FingerInfo responsibleNode = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(Utils.getKey(file)).build());
        ChordClient responsibleClient = new ChordClient(responsibleNode.getIp(), responsibleNode.getPort());
        Iterator<FileRequest> fileChunks;
        try {
            fileChunks = responsibleClient.blockingStub.downloadFile(TargetId.newBuilder().setId(Utils.getKey(file)).build());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        OutputStream writer = null;

        while (fileChunks.hasNext()) {
            try {
                FileRequest chunk = fileChunks.next();
                if (chunk.hasMetadata()) {
                    writer = Utils.getFilePath(chunk);
                } else {
                    if (writer == null) {
                        throw new IOException("writer not properly initialized");
                    }
                    Utils.writeFile(writer, chunk.getFile().getContent());
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error writing file: " + e.getMessage());
            }
        }
        Utils.closeFile(writer);
    }

    // TODO: download address should choose from finger table
    public void sendFile(String path) throws IOException, InterruptedException {
        FingerInfo successor = this.getSuccessor();
        final CountDownLatch finishLatch = new CountDownLatch(1);
        ChordClient chordClient = new ChordClient(successor.getIp(), successor.getPort());
        logger.info("will try to upload file " + path + " to " + successor.getIp() + ":" + successor.getPort());
        StreamObserver<FileRequest> streamObserver = chordClient.asyncStub.upload(new FileUploadObserver(finishLatch));

        Path p = Paths.get(path);

//        File file = new File(path);
        FileRequest metadata = FileRequest.newBuilder()
                .setMetadata(MetaData.newBuilder()
                        .setName(p.getFileName().toString()))
//                        .setType( p.get().toString()).build())
                .build();
        streamObserver.onNext(metadata);
        // upload bytes
        InputStream inputStream = Files.newInputStream(p);
        byte[] bytes = new byte[4096];
        int size;
        while ((size = inputStream.read(bytes)) > 0) {
            FileRequest uploadRequest = FileRequest.newBuilder()
                    .setFile(FilePacket.newBuilder().setContent(ByteString.copyFrom(bytes, 0, size)).build())
                    .build();
            streamObserver.onNext(uploadRequest);
        }
        // close the stream
        inputStream.close();
        streamObserver.onCompleted();
        finishLatch.await();
//        chordClient.shutdown();
    }

    public FingerInfo findPredecessor(long id) {
        GetSuccessorRequest request = GetSuccessorRequest.newBuilder().build();
        FingerInfo nDash = this.getSelfFingerInfo();

        while (true) {
            ChordClient chordClient = new ChordClient(nDash.getIp(), nDash.getPort());
            try {
                FingerInfo nDashSuccessor = chordClient.blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS).getSuccessor(request);
                if (!Utils.inside(id, nDash.getId(), nDashSuccessor.getId(), false, true)) {
                    TargetId targetId = TargetId.newBuilder().setId(id).build();
                    nDash = chordClient.blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS).closestPrecedingFinger(targetId);

                } else {
                    break;
                }
            } catch (StatusRuntimeException e) {
                logger.log(Level.SEVERE, "request failed: " + e.getMessage());
                return null;
            }
//            finally {
//                chordClient.shutdown();
////                try {
////                    chordClient.shutdown();
////                } catch (InterruptedException e) {
////                    e.printStackTrace();
////                }
//            }
        }
        logger.info("findPredecessor result: " + nDash);
        return nDash;
    }

    public FingerInfo closestPrecedingFinger(long id) {
//        printFTable();
        for (int i = Node.M - 1; i >= 0; i--) {
            if (Utils.inside(this.fingerTable.get(i).getId(), this.id, id, false, false)) {
                return this.fingerTable.get(i);
            }
        }
        return this.getSelfFingerInfo();
    }

    private FingerInfo getSelfFingerInfo() {
        return FingerInfo.newBuilder().setIp(this.ip).setPort(this.port).setId(this.id).build();
    }

    void printFTable() {
        if (this.fingerTable == null || this.fingerTable.size() == 0) {
            System.out.println("Finger Table is empty, please join first");
            return;
        }
        System.out.println("<--------------------  Finger Table: --------------------->");
        for (int i = 0; i < M; i++) {
            System.out.println("Finger " + i + ": " + this.fingerTable.get(i).getId() + " " + this.fingerTable.get(i).getIp() + " " + this.fingerTable.get(i).getPort());
        }
        System.out.println("<--------------------------------------------------------->");
    }

    void printFileMap() {
        if (this.fileMap == null || this.fileMap.size() == 0) {
            System.out.println("File Map is empty, please join first");
            return;
        }
        System.out.println("<--------------------  File Map: --------------------->");
        for (Map.Entry<Long, String> entry : this.fileMap.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
        System.out.println("<--------------------------------------------------------->");
    }

    private static class FileUploadObserver implements StreamObserver<FileUploadResponse> {

        private final CountDownLatch latch;

        public FileUploadObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onNext(FileUploadResponse fileUploadResponse) {
            System.out.println(
                    "File upload status :: " + fileUploadResponse.getStatus()
            );
        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();
        }

        @Override
        public void onCompleted() {
            System.out.println("File upload completed (success)");
            this.latch.countDown();
        }
    }


}
