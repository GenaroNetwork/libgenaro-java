package network.genaro.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileUtils {
    public static byte[] getBlock(long offset, File file, int blockSize) throws IOException {
        byte[] result = new byte[blockSize];

        try (RandomAccessFile accessFile = new RandomAccessFile(file, "r")) {
            accessFile.seek(offset);

            int readSize = accessFile.read(result);
            if (readSize == -1) {
                return null;
            } else if (readSize == blockSize) {
                return result;
            } else {
                byte[] tmpByte = new byte[readSize];
                System.arraycopy(result, 0, tmpByte, 0, readSize);
                return tmpByte;
            }
        }
    }
}
