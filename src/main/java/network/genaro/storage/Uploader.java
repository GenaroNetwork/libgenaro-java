package network.genaro.storage;

import org.bouncycastle.util.encoders.Hex;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutionException;

import static network.genaro.storage.CryptoUtil.BUCKET_META_MAGIC;
import static network.genaro.storage.CryptoUtil.BUCKET_NAME_MAGIC;
import static network.genaro.storage.CryptoUtil.string2Bytes;

public class Uploader {

    private String path;
    private BridgeApi bridge;
    private String bucketId;
    private Progress progress;
    private File originFile;

    private static long MAX_SHARD_SIZE = 4294967296L; // 4Gb
    private static long MIN_SHARD_SIZE = 2097152L; // 2Mb
    private static int SHARD_MULTIPLES_BACK = 4;

    public Uploader(final BridgeApi bridge, String filePath, String bucketId, Progress progress) {
        this.bridge = bridge;
        this.path = filePath;
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

    public void start() throws Exception {
        long shardSize = determinShardSize(originFile.length(), 0);

        String name = originFile.getName();
        byte[] bucketKey = CryptoUtil.generateBucketKey(bridge.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, string2Bytes(name));

        String encryptedFileName = CryptoUtil.encryptMeta(string2Bytes(name), key, nameIv);


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

        // queue_prepare_frame s

        // upload frame and shards

        // queue_create_bucket_entry

        // send report
    }
}
