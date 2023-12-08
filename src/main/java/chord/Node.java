package chord;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

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
import java.util.stream.Collectors;


public class Node extends ChordGrpc.ChordImplBase {
    private static final Logger logger = Logger.getLogger(Node.class.getName());
    public static final boolean DEBUG = false;
    public static final int M = 3; // bits of hash
    public static final int MAX_NUMBER_NODE = (int) Math.pow(2, M); // 2^M
    public static final int PERIODICALLY_CHECK_INTERVAL = 5000; // 5s
    public static final int R = 3; // number of successors

    private final String ip;
    private final int port;
    private final int id;


    private final Map<Integer, FingerInfo> fingerTable;
    private final List<Integer> fingerStart; // (n+2^(k-1) % 2^m), 1 <= k <= m
    private final Map<Long, String> fileMap; // 存储其负责的标识符，String is key
    private FingerInfo predecessor;
    private final FingerInfo[] successorList; // keep track of the next r successors


    public Node() {
        this(Integer.parseInt(System.getenv("PORT")));
    }

    public Node(int port) {
        this("127.0.0.1", port);
    }

    public Node(String ip, int port) {
        System.out.println("running on port:" + port);
        this.ip = ip;
        this.port = port;
        this.id = Utils.getKey(ip, port);
        this.fingerTable = new HashMap<>();
        this.fingerStart = new ArrayList<>();
        this.fileMap = new HashMap<>();
        for (int k = 0; k <= M; k++) {
            this.fingerStart.add((int) ((this.id + Math.pow(2, k)) % MAX_NUMBER_NODE));
        }
        this.successorList = new FingerInfo[R];
    }

    public void start() {
        userInputHandler();
    }

    private void userInputHandler() {
        boolean joined = false;
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter text. Type 'exit' to quit.");
        while (true) {
            System.out.print(App.IP + ":" + App.PORT + ":p2p_bash>");
            String input = "";
            if (scanner.hasNextLine()) {
                input = scanner.nextLine();
            }

            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Exiting program.");
                this.leave();
                System.exit(0);
            }

            String[] inputArray = input.split("\\s+");
            String command = inputArray[0];
            if (command.equalsIgnoreCase("join") && !joined) {
                System.out.println("Join Chord ring...");
                if (inputArray.length == 1) {
                    this.join();
                    joined = true;
                } else if (inputArray.length == 3) {
                    this.join(inputArray[1], Integer.parseInt(inputArray[2]));
                    System.out.println("Joined Chord ring.");
                    joined = true;
                }
                continue;
            }
            if (!joined) {
                System.out.println("Please join the ring first.");
                System.out.println("-- join: start a new ring");
                System.out.println("-- join [<ip>] [<port>]: join chord ring, if no ip and port provided, start new ring");
                continue;
            }
            if (command.equalsIgnoreCase("upload") && inputArray.length == 2) {
                try {
                    System.out.println("Uploading file...");
                    this.sendFile(inputArray[1]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (command.equalsIgnoreCase("ftable") && inputArray.length == 1) {
                this.printFTable();
            } else if (command.equalsIgnoreCase("files") && inputArray.length == 1) {
                this.printFileMap();
            } else if (command.equalsIgnoreCase("download") && inputArray.length == 2) {
                this.downloadFile(inputArray[1]);
            } else if (command.equalsIgnoreCase("info") && inputArray.length == 1) {
                this.printInfo();
            } else {
                System.out.println("Help:");
                System.out.println("[command] [args]");
                System.out.println("-- upload <file_path>: upload file to chord ring");
                System.out.println("-- join: start a new ring");
                System.out.println("-- join [<ip>] [<port>]: join chord ring, if no ip and port provided, start new ring");
                System.out.println("-- ftable: print finger table");
                System.out.println("-- files: print file stored in this map and corresponding key");
                System.out.println("-- download <file_name>: download file from chord");
                System.out.println("-- info: print node info");
                System.out.println("-- exit: exit program");
            }
        }
    }

    public void printInfo() {
        System.out.println(this);
    }

    @Override
    public String toString() {
        List<String> ftableList = this.fingerTable.entrySet().stream().map(entry -> "    " + entry.getKey() + "->" + Utils.formatFingerInfo(entry.getValue()) + "\n").toList();
        String ftable = String.join("", ftableList);
        List<String> filesList = this.fileMap.entrySet().stream().map(entry -> "    " + entry.getValue() + ":" + entry.getKey() + "\n").toList();
        String files = String.join("", filesList);
        String successors;
        if (this.successorList[0] != null) {
            List<String> successorList = Arrays.stream(this.successorList)
                    .map(entry -> "    " + (entry != null ? Utils.formatFingerInfo(entry) : "null") + "\n")
                    .collect(Collectors.toList());
            successors = String.join("", successorList);
        } else {
            successors = "    null\n";
        }

        return "Node " + this.getId() + " {\n" +
                "  address=" + ip + ":" + port + ", id=" + id + ",\n" +
                "  fingerTable=\n" + ftable +
                "  fileMap=\n" + files +
                "  predecessor=" + Utils.formatFingerInfo(predecessor) + "\n" +
                "  successorList=\n" + successors + "\n" +
                '}';
    }

    private void runCheckingThread() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Node.this.fixFingers();
                Node.this.stabilize();
            }
        }, 3000, Node.PERIODICALLY_CHECK_INTERVAL);
    }


    // 都是issue rpc 的, client端
    // first join the ring
    public void join() {
        for (int i = 0; i < M; i++) {
            this.setFingerEntry(i, FingerInfo.newBuilder().setIp(this.ip).setPort(this.port).setId(this.id).build());
        }
        this.predecessor = FingerInfo.newBuilder().setIp(this.ip).setPort(this.port).setId(this.id).build();
        this.successorList[0] = FingerInfo.newBuilder().setIp(this.ip).setPort(this.port).setId(this.id).build();
        this.runCheckingThread();
    }


    public void join(String remoteAddress, int remotePort) {
        try {
            this.initFingerTable(remoteAddress, remotePort);
        } catch (StatusRuntimeException e) {
            System.out.println("Failed to join the ring, please check the ip and port");
            return;
        }
        this.updateOthers();
        this.copyKeys();
        this.runCheckingThread();
    }

    public void leave() {
        // notify predecessor and successor
        FingerInfo successor = this.getSuccessor();
        FingerInfo predecessor = this.getPredecessor();
        ChordClient successorClient = new ChordClient(successor.getIp(), successor.getPort());
        ChordClient predecessorClient = new ChordClient(predecessor.getIp(), predecessor.getPort());
        ResponseStatus pStatus;
        ResponseStatus sStatus;
        try {
            pStatus = predecessorClient.blockingStub.setSuccessor(successor);
            sStatus = successorClient.blockingStub.setPredecessor(predecessor);
        } finally {
            predecessorClient.shutdown();
            successorClient.shutdown();
        }
        for (String file : this.fileMap.values()) {
            try {
                this.sendFile(String.valueOf(App.SERVER_BASE_PATH.resolve(file)));
            } catch (IOException e) {
                System.out.println("file cannot open due to some io exception: " + file);
                e.printStackTrace();
            } catch (InterruptedException e) {
                System.out.println("file cannot open due to some interrupted exception: " + file);
                e.printStackTrace();
            }
        }
        // when exit, it is optionally to delete all files stored in current node
        this.updateOthersWhenLeave();
    }


    // initialize finger table of  local node, the paper states no need to let chord network aware of newly joined node in section 5.1, which means no need to notify its successor and predecessor
    public void initFingerTable(String remoteAddress, int remotePort) {
//        System.out.println("remoteAddress: " + remoteAddress + " remotePort: " + remotePort);
        ChordClient chordClient = new ChordClient(remoteAddress, remotePort);
        try {
            FingerInfo successor = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(this.id).build());
//            this.fingerTable.put(0, successor);
            this.setSuccessor(successor);
//        System.out.println("successor: " + successor.toString());
            ChordClient successorClient = new ChordClient(this.getSuccessor().getIp(), this.getSuccessor().getPort());
            FingerInfo predecessor = successorClient.blockingStub.getPredecessor(GetPredecessorRequest.newBuilder().build());

            this.predecessor = predecessor;
            ChordClient predecessorClient = new ChordClient(predecessor.getIp(), predecessor.getPort());
            ResponseStatus pStatus;
            ResponseStatus sStatus;
            try {
                pStatus = predecessorClient.blockingStub.setSuccessor(this.getSelfFingerInfo());
                sStatus = successorClient.blockingStub.setPredecessor(this.getSelfFingerInfo());
            } catch (StatusRuntimeException e) {
                System.out.println("cannot connect predecessorClient, ip: " + predecessor.getIp() + " port: " + predecessor.getPort());
            } finally {
                predecessorClient.shutdown();
                successorClient.shutdown();
            }

            if (DEBUG) {
                logger.info("initFingerTable result: pStatus" + pStatus.getStatus() + ", sStatus" + sStatus.getStatus());
            }

            for (int i = 0; i < M - 1; i++) {
                FingerInfo newFinger = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(this.fingerStart.get(i + 1)).build());
                this.fingerTable.put(i + 1, newFinger);

            }
        } catch (StatusRuntimeException e) {
            System.out.println("Failed to init finger table, please check the ip and port");
        } finally {
            chordClient.shutdown();
        }
    }

    // update all nodes whose finger tables should refer to this node
    private void updateOthers() {
//        this.printFTable();
        for (int i = 0; i < M; i++) {
            // find last node p whose i_th finger might be this node
            int prev;
            if (this.getId() >= Math.pow(2, i)) {
                prev = this.getId() - (int) Math.pow(2, i);
            } else {
                prev = MAX_NUMBER_NODE - (int) Math.pow(2, i) + this.getId();
            }
            FingerInfo p = this.findPredecessor(prev);
            ChordClient chordClient = new ChordClient(p.getIp(), p.getPort());
            try {
                ResponseStatus response = chordClient.blockingStub.updateFingerTable(UpdateFingerRequest.newBuilder().setFinger(this.getSelfFingerInfo()).setIndex(i).build());

            } finally {
                chordClient.shutdown();
            }
        }
    }

    // update all nodes whose finger tables should refer to this node when leave the ring
    public void updateOthersWhenLeave() {
        FingerInfo successor = this.getSuccessor();
        for (int i = 0; i < M; i++) {
            int prev;
            if (this.getId() >= Math.pow(2, i)) {
                prev = this.getId() - (int) Math.pow(2, i);
            } else {
                prev = MAX_NUMBER_NODE - (int) Math.pow(2, i) + this.getId();
            }
            FingerInfo p = this.findPredecessor(prev);
            ChordClient chordClient = new ChordClient(p.getIp(), p.getPort());
            try {
                ResponseStatus response = chordClient.blockingStub.updateFingerTable(UpdateFingerRequest.newBuilder().setFinger(successor).setIndex(i).build());

            } finally {
                chordClient.shutdown();
            }
        }
    }

    public void stabilize() {
//        System.out.println("stabilize called");
        // always start from the node itself
        FingerInfo successor = this.getSuccessor();
        if (successor == null) {
            return;
        }
        ChordClient successorClient = new ChordClient(successor.getIp(), successor.getPort());
        FingerInfo x;
        FingerInfo successorSuccessors; // TODO: change it to a list
        int i = 0;
        try {
            x = successorClient.blockingStub.getPredecessor(GetPredecessorRequest.newBuilder().build());
            if (Utils.inside(x.getId(), this.id, successor.getId(), false, false)) {
                this.setSuccessor(x);
            }
            this.notifyOther(successor);
            // update successorClient
            successorClient = new ChordClient(this.successorList[0].getIp(), this.successorList[0].getPort());
            // loop whole successorlist and update
            for (i = 1; i < this.successorList.length; i++) {

                successorSuccessors = successorClient.blockingStub.getSuccessor(GetSuccessorRequest.newBuilder().build());
//                System.out.println(i);
//                System.out.println(successorSuccessors.getPort());
                // init
                if (this.successorList[i] == null) {
                    this.successorList[i] = successorSuccessors;
                    successorClient = new ChordClient(successor.getIp(), successor.getPort());
                    continue;
                }
                // update a better one
                this.setSuccessor(i, successorSuccessors);
//                x = successorClient.blockingStub.getPredecessor(GetPredecessorRequest.newBuilder().build());
//                if (Utils.inside(x.getId(), this.id, successor.getId(), false, false)) {
//                    this.setSuccessor(i, x);
//                }
                this.notifyOther(successor);
                // update successorClient
                successorClient = new ChordClient(this.successorList[i].getIp(), this.successorList[i].getPort());
            }
        }
        catch (StatusRuntimeException e) {
            // connection is shutdown, we assume that the node is down and delete that node in finger table
            System.out.println("one successor down, stabilizing");
            // remove it from the list and shift others up
            for (int j = i; j < this.successorList.length-1; j++) {
                this.successorList[j] = this.successorList[j+1];
            }
            this.successorList[this.successorList.length-1] = null;
            for (int k = 0; k< this.successorList.length; k++){
                System.out.println("current successor list are"+this.successorList[k]);
            }
            // add new successor at the end of the list
//            successorClient = new ChordClient(this.successorList[this.successorList.length-2].getIp(), this.successorList[this.successorList.length-2].getPort());
//            successorSuccessors = successorClient.blockingStub.getSuccessor(GetSuccessorRequest.newBuilder().build());
//            this.setSuccessor(successorList.length-1, successorSuccessors);
        }
        finally {
            successorClient.shutdown();
        }


    }


    private void notifyOther(FingerInfo successor) {
        ChordClient newSuccessorClient = new ChordClient(successor.getIp(), successor.getPort());
        try {
            ResponseStatus responseStatus = newSuccessorClient.blockingStub.notify(this.getSelfFingerInfo());
        } finally {
            newSuccessorClient.shutdown();
        }
//        System.out.println("notifyOther result: " + responseStatus.getStatus());
    }

    public void fixFingers() {

        int index = ThreadLocalRandom.current().nextInt(1, M);
        ChordClient chordClient = new ChordClient(this.ip, this.port);
        FingerInfo fingerInfo;
        // if connection is shutdown, we assume that the node is down and delete that node in finger table and find a new one

        try {
            fingerInfo = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(this.fingerStart.get(index)).build());
            this.setFingerEntry(index, fingerInfo);
        } catch (StatusRuntimeException e) {
            System.out.println("connection is shutdown, we assume that the node is down and delete that node in finger table and find a new one");

        } finally {
            chordClient.shutdown();
        }
    }

    /**
     * calculate all files that should be responsible for the new node with the given id, this is a helper for server side chord. When a newly joined node, it will ask its success (this node) to transfer files to itself
     *
     * @param id the id of the new node
     * @return a list of file names that should be responsible for the new node
     */
    public List<String> getKeys(long id) {
        List<String> keys = new ArrayList<>();
        for (Map.Entry<Long, String> entry : this.fileMap.entrySet()) {
            if (Utils.inside(entry.getKey(), this.getId(), id, false, true)) {
//                System.out.println("file"+ entry.getValue() + "should be responsible for the new node" + id);
//                System.out.println("file id: " + entry.getKey() + "predecessor id: " + this.predecessor.getId() + "new node id: " + id);
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
        // TODO: match setSuccessorList
        this.fingerTable.put(index, fingerInfo);
//
    }

    public void setSuccessor(FingerInfo successor) {
        this.fingerTable.put(0, successor);
        this.successorList[0] = successor;
    }

    public void setSuccessor(int index, FingerInfo fingerInfo) {
        // TODO: match setSuccessorList
        this.successorList[index] = fingerInfo;

//            this.fingerTable.put(index, fingerInfo);

    }


    public String getFileName(long key) {
        return this.fileMap.get(key);
    }

    public FingerInfo getSuccessor() {
//        return this.fingerTable.get(0);
        return this.successorList[0];
    }


    public FingerInfo getPredecessor() {
        return this.predecessor;
    }

    public void setPredecessor(FingerInfo predecessor) {
        this.predecessor = predecessor;
    }

    public void copyKeys() {
        // TOOD: copy keys 不对
        ChordClient chordClient = new ChordClient(this.getSuccessor().getIp(), this.getSuccessor().getPort());
        Iterator<FileRequest> fileChunks;
        try {
            fileChunks = chordClient.blockingStub.moveFiles(TargetId.newBuilder().setId(this.id).build());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        } finally {
            chordClient.shutdown();
        }
        OutputStream writer = null;
//        List<String> fileNames = new ArrayList<>();
        while (fileChunks.hasNext()) {
            String fileName = "";
            try {
                FileRequest chunk = fileChunks.next();
//                System.out.println("chunk: " + chunk.toString());
                if (chunk.hasMetadata() && writer == null) {
                    fileName = chunk.getMetadata().getName();
                    this.addKey(Utils.getKey(fileName), fileName);
                    writer = Utils.getFilePath(chunk);
                } else if (chunk.hasMetadata()) {
                    Utils.closeFile(writer);
                    fileName = chunk.getMetadata().getName();
                    this.addKey(Utils.getKey(fileName), fileName);
                    writer = Utils.getFilePath(chunk);

                } else {
                    if (writer == null) {
                        this.removeKey(Utils.getKey(fileName));
                        throw new IOException("writer not properly initialized");
                    } else {
                        Utils.writeFile(writer, chunk.getFile().getContent());
                    }
                }
            } catch (IOException e) {
                this.removeKey(Utils.getKey(fileName));
                logger.log(Level.SEVERE, "Error writing file: " + e.getMessage());
            }
        }
        try {
            if (writer != null) {
                Utils.closeFile(writer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void downloadFile(String file) {
        FingerInfo successor = this.getSuccessor();
        ChordClient chordClient = new ChordClient(successor.getIp(), successor.getPort());
        FingerInfo responsibleNode;
        try {
            responsibleNode = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(Utils.getKey(file)).build());
        } finally {
            chordClient.shutdown();
        }

        ChordClient responsibleClient = new ChordClient(responsibleNode.getIp(), responsibleNode.getPort());
        logger.info("downloadFile called, fetch file from " + responsibleNode + " for file " + file);
        Iterator<FileRequest> fileChunks;
        try {
            fileChunks = responsibleClient.blockingStub.downloadFile(TargetId.newBuilder().setId(Utils.getKey(file)).build());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        } finally {
            responsibleClient.shutdown();
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
        try {
            if (writer != null) {
                Utils.closeFile(writer);
                System.out.println("File downloaded success. ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void sendFile(String path) throws IOException, InterruptedException {
//        this.lookup();
        ChordClient chordClient = new ChordClient(this.ip, this.port);
        FingerInfo responsibleNode = chordClient.blockingStub.findSuccessor(TargetId.newBuilder().setId(Utils.getKey(path)).build());
//        FingerInfo successor = this.getSuccessor();
        final CountDownLatch finishLatch = new CountDownLatch(1);
        ChordClient responsibleClient = new ChordClient(responsibleNode.getIp(), responsibleNode.getPort());
        logger.info("will try to upload file " + path + " to " + responsibleNode.getIp() + ":" + responsibleNode.getPort());
        StreamObserver<FileRequest> streamObserver = responsibleClient.asyncStub.upload(new FileUploadObserver(finishLatch));

        Path p = Paths.get(path);


        FileRequest metadata = FileRequest.newBuilder()
                .setMetadata(MetaData.newBuilder()
                        .setName(p.getFileName().toString()))
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
        chordClient.shutdown();
    }

    public FingerInfo findPredecessor(long id) {
        if (id == this.id) {
            return this.getPredecessor();
        }
        GetSuccessorRequest request = GetSuccessorRequest.newBuilder().build();
        FingerInfo nDash = this.getSelfFingerInfo();

        while (true) {
            ChordClient chordClient = new ChordClient(nDash.getIp(), nDash.getPort());
            try {
                FingerInfo nDashSuccessor = chordClient.blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS).getSuccessor(request);
                if (!Utils.inside(id, nDash.getId(), nDashSuccessor.getId(), false, true)) {
                    TargetId targetId = TargetId.newBuilder().setId(id).build();
                    nDash = chordClient.blockingStub.closestPrecedingFinger(targetId);
//                    nDash = chordClient.blockingStub.withDeadlineAfter(5, TimeUnit.SECONDS).closestPrecedingFinger(targetId);

                } else {
                    break;
                }
            } catch (StatusRuntimeException e) {
                e.printStackTrace();
//                logger.log(Level.SEVERE, "request failed: " + e.getMessage());
                return nDash;
            } finally {
                chordClient.shutdown();
            }
        }
        if (DEBUG) {
            logger.info("findPredecessor result: " + nDash);

        }
        return nDash;
    }

    public FingerInfo closestPrecedingFinger(long id) {

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
        for (Map.Entry<Integer, FingerInfo> entry : this.fingerTable.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().getId() + " " + entry.getValue().getIp() + ":" + entry.getValue().getPort());
        }
        System.out.println("<--------------------------------------------------------->");
    }

    void printFileMap() {
        if (this.fileMap == null || this.fileMap.size() == 0) {
            System.out.println("File Map is empty, no files stored in this node");
            return;
        }
        System.out.println("<----------------------  File Map: ----------------------->");
        for (Map.Entry<Long, String> entry : this.fileMap.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
        System.out.println("<--------------------------------------------------------->");
    }

    private record FileUploadObserver(
            CountDownLatch latch) implements StreamObserver<FileUploadResponse> {

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
