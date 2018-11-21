package network.genaro.storage;

import org.bouncycastle.util.encoders.Hex;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;
import static network.genaro.storage.CryptoUtil.BUCKET_META_MAGIC;
import static network.genaro.storage.CryptoUtil.BUCKET_NAME_MAGIC;
import static network.genaro.storage.CryptoUtil.string2Bytes;

public class Uploader {

    private String path;
    private String encryptedPath;
    private BridgeApi bridge;
    private String bucketId;
    private Progress progress;
    private File originFile;

    private static Random random = new Random();
    private static long MAX_SHARD_SIZE = 4294967296L; // 4Gb
    private static long MIN_SHARD_SIZE = 2097152L; // 2Mb
    private static int SHARD_MULTIPLES_BACK = 4;
    private static int GENARO_SHARD_CHALLENGES = 4;

    public Uploader(final BridgeApi bridge, String filePath, String bucketId, Progress progress) {
        this.bridge = bridge;
        this.path = filePath;
        this.encryptedPath = filePath + ".encrypted";
        this.originFile = new File(filePath);
        this.bucketId = bucketId;
        this.progress = progress;
    }

    private static long shardSize(int hops) {
        return (long) (MIN_SHARD_SIZE * Math.pow(2, hops));
    }

    private static long determinShardSize(long fileSize, int accumulator) {

        if (fileSize <= 0) {
            return 0;
        }

        accumulator = accumulator > 0 ? accumulator : 0;

        // Determine hops back by accumulator
        int hops = ((accumulator - SHARD_MULTIPLES_BACK) < 0 ) ? 0 : accumulator - SHARD_MULTIPLES_BACK;

        long byte_multiple = shardSize(accumulator);
        double check = (double) fileSize / byte_multiple;

        // Determine if bytemultiple is highest bytemultiple that is still <= size
        if (check > 0 && check <= 1) {
            while (hops > 0 && shardSize(hops) > MAX_SHARD_SIZE) {
                hops = hops - 1 <= 0 ? 0 : hops - 1;
            }
            return shardSize(hops);
        }

        // Maximum of 2 ^ 41 * 8 * 1024 * 1024
        if (accumulator > 41) {
            return 0;
        }

        return determinShardSize(fileSize, ++ accumulator);
    }

    private static byte[] randomBuff(int len) {
        byte[] buff = new byte[len];
        random.nextBytes(buff);
        return buff;
    }

    public void start() throws Exception {
        long shardSize = determinShardSize(originFile.length(), 0);
        long fileSize = originFile.length();
        // total shard

        int totalDataShards = (int) (fileSize / shardSize + (fileSize % shardSize == 0 ? 0 : 1));

        String name = originFile.getName();
        byte[] bucketKey = CryptoUtil.generateBucketKey(bridge.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, string2Bytes(name));

        String encryptedFileName = CryptoUtil.encryptMeta(string2Bytes(name), key, nameIv);


        // aes
        //generate IV
        byte[] index   = randomBuff(32);
        byte[] fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), Hex.decode(bucketId), index);
        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(ENCRYPT_MODE, keySpec, iv);

        try (InputStream in = new FileInputStream(this.path);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            Files.copy(cypherIn, Paths.get(this.encryptedPath));
        }

        // queue_verify_bucket_id
        try {
            bridge.getBucket(bucketId).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            // throw new Exception("bucket not exist");
            return;
        }

        // queue_verify_file_name

        if (bridge.isFileExist(bucketId, encryptedFileName).get()) {
            // throw new Exception("file exists");
            return;
        }
        // queue_request_frame_id
        Frame frame = bridge.requestNewFrame().get();
        String frameId = frame.getId();

        for (int shardi = 0; shardi < totalDataShards; shardi ++) {
            // queue_prepare_frame s
            List<byte[]> challenges = new ArrayList<>(GENARO_SHARD_CHALLENGES);
            for (int i = 0; i < GENARO_SHARD_CHALLENGES; i ++) {
                challenges.set(i, randomBuff(32));
            }
        }
        // Calculate the merkle tree with challenges


        // upload frame and shards

        // queue_create_bucket_entry

        // send report
    }
}
