package chord;


import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Logger;

public class ChordService extends ChordGrpc.ChordImplBase {
    private static final Logger logger = Logger.getLogger(ChordService.class.getName());
    public static final Path SERVER_BASE_PATH = Paths.get("src/main/resources/" + System.getenv("PORT") + "/");
    private final Node node;

    public ChordService(int port) {
        this.node = new Node(port);
    }

    public Node getNode() {
        return node;
    }

    /**
     * @param request          the request contains the info of the node that thinks it might be our predecessor
     * @param responseObserver the responseObserver is used to send the response back to the caller
     */
    @Override
    public void notify(FingerInfo request, StreamObserver<ResponseStatus> responseObserver) {
        long requestId = request.getId();
        FingerInfo currentPredecessor = this.node.getPredecessor();
        if (currentPredecessor == null || Utils.inside(requestId, currentPredecessor.getId(), this.node.getId(), false, false)) {
            this.node.setPredecessor(request);
            responseObserver.onNext(ResponseStatus.newBuilder().setStatus(ResponseStatus.Status.SUCCESS).build());
        }else{
            responseObserver.onNext(ResponseStatus.newBuilder().setStatus(ResponseStatus.Status.FAILED).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    // move keys in (predecessor,n] from successor
    public void moveFiles(TargetId request, StreamObserver<FileRequest> responseObserver) {
        List<String> files = this.node.getKeys(request.getId());
        for (String fileName : files) {
            String filePath = SERVER_BASE_PATH + fileName;

            FileRequest metadata = FileRequest.newBuilder()
                    .setMetadata(MetaData.newBuilder()
                            .setName(fileName))
                    .build();
            responseObserver.onNext(metadata);
            try {
                // read the file and convert to a byte array
                InputStream inputStream = Files.newInputStream(Path.of(filePath));
                byte[] bytes = new byte[4096];
                int size;
                while ((size = inputStream.read(bytes)) > 0) {
                    FileRequest uploadRequest = FileRequest.newBuilder()
                            .setFile(FilePacket.newBuilder().setContent(ByteString.copyFrom(bytes, 0, size)).build())
                            .build();
                    if (!requiredNonCancelled(responseObserver)) {
                        return;
                    }
                    responseObserver.onNext(uploadRequest);
                }
                // close the stream
                inputStream.close();
                this.node.removeKey(Utils.getKey(fileName));
            } catch (IOException e) {
                responseObserver.onError(Status.ABORTED
                        .withDescription("Unable to acquire the file " + fileName)
                        .withCause(e)
                        .asException());
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public void downloadFile(TargetId request, StreamObserver<FileRequest> responseObserver) {
        String fileName = this.node.getFileName(request.getId());
        Path filePath = SERVER_BASE_PATH.resolve(fileName);
        System.out.println(filePath);
        FileRequest metadata = FileRequest.newBuilder()
                .setMetadata(MetaData.newBuilder()
                        .setName(fileName))
                .build();
        responseObserver.onNext(metadata);
        try {
            // read the file and convert to a byte array
            InputStream inputStream = Files.newInputStream(filePath);
            byte[] bytes = new byte[4096];
            int size;
            while ((size = inputStream.read(bytes)) > 0) {
                FileRequest uploadRequest = FileRequest.newBuilder()
                        .setFile(FilePacket.newBuilder().setContent(ByteString.copyFrom(bytes, 0, size)).build())
                        .build();
                if (!requiredNonCancelled(responseObserver)) {
                    return;
                }
                responseObserver.onNext(uploadRequest);
            }

            // close the stream
            inputStream.close();
            responseObserver.onCompleted();

        } catch (IOException e) {
            responseObserver.onError(Status.ABORTED
                    .withDescription("Unable to acquire the file " + fileName)
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public StreamObserver<FileRequest> upload(StreamObserver<FileUploadResponse> responseObserver) {
        return new StreamObserver<>() {
            // upload context variables
            OutputStream writer;
            String fileName;
            FileStatus status = FileStatus.IN_PROGRESS;

            @Override
            public void onNext(FileRequest fileUploadRequest) {
                try {
                    // TODO: check if file hash is on its responsibility
                    if (fileUploadRequest.hasMetadata()) {

                        fileName = fileUploadRequest.getMetadata().getName();
                        writer = Utils.getFilePath(fileUploadRequest);
                    } else {
                        Utils.writeFile(writer, fileUploadRequest.getFile().getContent());
                    }
                } catch (IOException e) {
                    this.onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                status = FileStatus.FAILED;
                this.onCompleted();
            }

            @Override
            public void onCompleted() {
                logger.info("File upload completed");
                try {
                    Utils.closeFile(writer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                status = FileStatus.IN_PROGRESS.equals(status) ? FileStatus.SUCCESS : status;
                FileUploadResponse response = FileUploadResponse.newBuilder()
                        .setStatus(status)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                ChordService.this.node.addKey(Utils.getKey(fileName), fileName);
            }
        };
    }


    @Override
    public void findSuccessor(TargetId request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo n_dash = this.node.findPredecessor(request.getId());
        ChordClient client = new ChordClient(n_dash.getIp(), n_dash.getPort());
        FingerInfo fingerInfo = client.blockingStub.getSuccessor(GetSuccessorRequest.newBuilder().build());
        client.shutdown();
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        logger.info("findSuccessor: " + fingerInfo);
    }

    @Override
    public void getSuccessor(GetSuccessorRequest request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo fingerInfo = this.node.getSuccessor();
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        logger.info("getSuccessor: " + fingerInfo);
    }

    @Override
    public void getPredecessor(GetPredecessorRequest request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo fingerInfo = this.node.getPredecessor();
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        logger.info("getPredecessor: " + fingerInfo);
    }

    @Override
    public void setSuccessor(FingerInfo request, StreamObserver<ResponseStatus> responseObserver) {
        this.node.setSuccessor(request);
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        ResponseStatus.Status status = ResponseStatus.Status.SUCCESS;
        responseObserver.onNext(ResponseStatus.newBuilder().setStatus(status).build());
        responseObserver.onCompleted();
        logger.info("setSuccessor: " + status + ",request: " + request);
    }

    @Override
    public void setPredecessor(FingerInfo request, StreamObserver<ResponseStatus> responseObserver) {
        this.node.setPredecessor(request);
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        ResponseStatus.Status status = ResponseStatus.Status.SUCCESS;
        responseObserver.onNext(ResponseStatus.newBuilder().setStatus(status).build());
        responseObserver.onCompleted();
        logger.info("setPredecessor: " + status + ",request: " + request);
    }

    @Override
    public void closestPrecedingFinger(TargetId request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo fingerInfo = this.node.closestPrecedingFinger(request.getId());
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }

        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();

        logger.info("closestPrecedingFinger: " + fingerInfo);
    }

    @Override
    public void updateFingerTable(UpdateFingerRequest request, StreamObserver<ResponseStatus> responseObserver) {
        logger.info("updateFingerTable: " + request);
        FingerInfo fingerInfo = request.getFinger();
        int index = request.getIndex();
        ResponseStatus response;
        if (Utils.inside(fingerInfo.getId(), this.node.getId(), this.node.getFingerTable().get(index).getId(), true, false)) {

            this.node.setFingerEntry(index, fingerInfo);
            FingerInfo p = this.node.getPredecessor();
            ChordClient chordClient = new ChordClient(p.getIp(), p.getPort());
            response = chordClient.blockingStub.updateFingerTable(UpdateFingerRequest.newBuilder().setFinger(fingerInfo).setIndex(index).build());
            responseObserver.onNext(response);
            if (!requiredNonCancelled(responseObserver)) {
                return;
            }
        } else {
            response = ResponseStatus.newBuilder().setStatus(ResponseStatus.Status.FAILED).build();
            responseObserver.onNext(response);
        }

        responseObserver.onCompleted();

        logger.info("updateFingerTable: " + response);
    }

    private boolean requiredNonCancelled(StreamObserver<? extends GeneratedMessageV3> responseObserver) {
        if (Context.current().isCancelled()) {
            logger.info("request is cancelled");
            responseObserver.onError(
                    Status.CANCELLED
                            .withDescription("request is cancelled")
                            .asRuntimeException()
            );
            return false;
        }
        return true;
    }
}


