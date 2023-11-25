package chord;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {



    public static boolean inside(long key, long left, long right, boolean leftIncluded, boolean rightIncluded) {
//        if (key > Node.MAX_NUMBER_NODE || key < 0) {
//            System.out.println("inside params: key: " + key + ", left: " + left + ", right: " + right + ", leftIncluded: " + leftIncluded + ", rightIncluded: " + rightIncluded);
//
////            return false;
//            throw new IllegalArgumentException("key is not in range, key: " + key + ", left: " + left + ", right: " + right + ", leftIncluded: " + leftIncluded + ", rightIncluded: " + rightIncluded);
//        }
        if (left < right) {
            return (leftIncluded ? left - 1 : left) < key && key < (rightIncluded ? right + 1 : right);
        } else {
            return (leftIncluded ? left - 1 : left) < key || key < (rightIncluded ? right + 1 : right);
        }
    }


    public static int getKey(String ip, int port) {
        return getKey(ip + ":" + port);
    }


    // message is hash of the file name
    public static int getKey(String message) {

        MessageDigest md;
        try {
            md = MessageDigest.getInstance("sha1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return 0;
        }

        byte[] messageDigest = md.digest(message.getBytes());

        // Convert byte array into Dignum representation
        BigInteger no = new BigInteger(1, messageDigest);


        return Math.floorMod(no.intValue(), Node.MAX_NUMBER_NODE);
    }

    static OutputStream getFilePath(FileRequest request) throws IOException {
        String fileName = request.getMetadata().getName();

        return Files.newOutputStream(App.SERVER_BASE_PATH.resolve(fileName), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    static void writeFile(OutputStream writer, ByteString content) throws IOException {
        writer.write(content.toByteArray());
        writer.flush();
    }

    static void closeFile(OutputStream writer) throws IOException {
        writer.close();

    }

}
