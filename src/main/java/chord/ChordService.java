package chord;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Logger;

public class ChordService extends ChordGrpc.ChordImplBase {
    private static final Logger logger = Logger.getLogger(ChordService.class.getName());

    private final Node node;

    public ChordService() {
        this.node = new Node(App.IP, App.PORT);
        createFolder();

    }

    private void createFolder() {

        try {
            Files.createDirectory(App.SERVER_BASE_PATH);
            System.out.println("Folder created successfully!");
        } catch (FileAlreadyExistsException e) {
            System.out.println("Folder already exists.");
        } catch (IOException e) {
            System.err.println("Unable to create the folder: " + e.getMessage());
        }
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
        } else {
            responseObserver.onNext(ResponseStatus.newBuilder().setStatus(ResponseStatus.Status.FAILED).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    // move keys in (predecessor,n] from successor
    public void moveFiles(TargetId request, StreamObserver<FileRequest> responseObserver) {
        List<String> files = this.node.getKeys(request.getId());
        for (String fileName : files) {
            Path filePath = App.SERVER_BASE_PATH.resolve(fileName);
//            System.out.println("Sending file: " + filePath);
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
                this.node.removeKey(Utils.getKey(fileName));
                Files.delete(filePath);
            } catch (IOException e) {
                e.printStackTrace();
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
        Path filePath = App.SERVER_BASE_PATH.resolve(fileName);
//        System.out.println(filePath);
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
                    // TODO: check if file hash is on its responsibility, no need to implement right now, because we assume node is sending to the correct responsible node.
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
        FingerInfo fingerInfo = null;
        try {
            fingerInfo = client.blockingStub.getSuccessor(GetSuccessorRequest.newBuilder().build());
        } catch (StatusRuntimeException e) {
//            e.printStackTrace();
            System.out.println("Node " + n_dash + " is not available");
        } finally {
            client.shutdown();
        }
//        client.shutdown();
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        if (Node.DEBUG) {
            logger.info("findSuccessor: " + fingerInfo);
        }
    }


    @Override
    public void getSuccessor(GetSuccessorRequest request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo fingerInfo = this.node.getSuccessor();
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        if (Node.DEBUG) {
            logger.info("getSuccessor: " + fingerInfo);
        }
    }

    @Override
    public void getPredecessor(GetPredecessorRequest request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo fingerInfo = this.node.getPredecessor();
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        if (Node.DEBUG) {
            logger.info("getPredecessor: " + fingerInfo);
        }
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
        if (Node.DEBUG) {
            logger.info("setSuccessor: " + status + ",request: " + request);
        }
    }

    @Override
    public void setPredecessor(FingerInfo request, StreamObserver<ResponseStatus> responseObserver) {
        if (Node.DEBUG) {
            logger.info("setPredecessor: " + request);
        }
        this.node.setPredecessor(request);
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }
        ResponseStatus.Status status = ResponseStatus.Status.SUCCESS;
        responseObserver.onNext(ResponseStatus.newBuilder().setStatus(status).build());
        responseObserver.onCompleted();
        if (Node.DEBUG) {
            logger.info("setPredecessor: " + status + ",request: " + request);
        }
    }

    @Override
    public void closestPrecedingFinger(TargetId request, StreamObserver<FingerInfo> responseObserver) {
        FingerInfo fingerInfo = this.node.closestPrecedingFinger(request.getId());
        if (!requiredNonCancelled(responseObserver)) {
            return;
        }

        responseObserver.onNext(fingerInfo);
        responseObserver.onCompleted();
        if (Node.DEBUG) {
            logger.info("closestPrecedingFinger: " + fingerInfo);

        }
    }

    @Override
    public void updateFingerTable(UpdateFingerRequest request, StreamObserver<ResponseStatus> responseObserver) {
        if (Node.DEBUG) {
            logger.info("updateFingerTable: " + request);
        }
        FingerInfo fingerInfo = request.getFinger();
        int index = request.getIndex();
        ResponseStatus response;
        if (Utils.inside(fingerInfo.getId(), this.node.getId(), this.node.getFingerTable().get(index).getId(), true, false)) {

            this.node.setFingerEntry(index, fingerInfo);
            FingerInfo p = this.node.getPredecessor();
            ChordClient chordClient = new ChordClient(p.getIp(), p.getPort());
            try {
                response = chordClient.blockingStub.updateFingerTable(UpdateFingerRequest.newBuilder().setFinger(fingerInfo).setIndex(index).build());
            } finally {
                chordClient.shutdown();
            }
            responseObserver.onNext(response);
            if (!requiredNonCancelled(responseObserver)) {
                return;
            }
        } else {
            response = ResponseStatus.newBuilder().setStatus(ResponseStatus.Status.FAILED).build();
            responseObserver.onNext(response);
        }

        responseObserver.onCompleted();
        if (Node.DEBUG) {
            logger.info("updateFingerTable: " + response);
        }
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


