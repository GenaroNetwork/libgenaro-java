package network.genaro.storage;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import static javax.crypto.Cipher.DECRYPT_MODE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static network.genaro.storage.Parameters.*;

public class Downloader {
    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private String path;
    private String tempPath;
    private Genaro bridge;
    private String bucketId;
    private String fileId;
    private Progress progress;

    private long shardSize;
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    private boolean isDownloadError = false;

    // 使用 CachedThreadPool 比较耗内存，并发 200+的时候 会造成内存溢出
    // private static final ExecutorService shardExecutor = Executors.newCachedThreadPool();

    // 如果是CPU密集型应用，则线程池大小建议设置为N+1，如果是IO密集型应用，则线程池大小建议设置为2N+1
    private static final ExecutorService shardExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path, final Progress progress) {
        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        this.path = path;
        this.tempPath = path + ".genarotemp";
        this.progress = progress;
    }

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path) {
        this(bridge, bucketId, fileId, path, new Progress(){
            @Override
            public void onBegin() { }
            @Override
            public void onEnd(int status) { }
            @Override
            public void onProgress(float progress, String message) { }
        });
    }

    private CompletableFuture<ByteBuffer> downloadShardByPointer(final Pointer p) {
        return BasicUtil.supplyAsync(() -> {
            Farmer f = p.getFarmer();
            String url = String.format("http://%s:%s/shards/%s?token=%s", f.getAddress(), f.getPort(), p.getHash(), p.getToken());
            Request request = new Request.Builder().
                    url(url).
                    get().
                    build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (response.code() != 200) {
                    switch(code) {
                        case 401:
                        case 403:
                            throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FARMER_AUTH_ERROR));
                        case 504:
                            throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FARMER_TIMEOUT_ERROR));
                        default:
                            throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FARMER_REQUEST_ERROR));
                    }
                }
                byte[] body = response.body().bytes();
                return ByteBuffer.wrap(body);
            }
        }, shardExecutor);
    }

    public void start() throws GenaroRuntimeException, IOException, TimeoutException, ExecutionException, InterruptedException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        progress.onBegin();
        isDownloadError = false;
        FileChannel fileChannel = FileChannel.open(Paths.get(tempPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);

        // request info and pointers
        File file = bridge.getFileInfo(bucketId, fileId);

        List<Pointer> pointers = bridge.getPointers(bucketId, fileId);

        boolean hasMissingShard = pointers.stream().anyMatch(pointer -> pointer.isMissing());

//         TODO: isRs() is not the
//        boolean canRecoverShards = file.isRs();
        boolean canRecoverShards = false;

        if(file.isRs()) {
            int missingPointers = (int) pointers.stream().filter(pointer -> pointer.isMissing()).count();

            // TODO:
//        if(missingPointers <= total_parity_pointers) {
//            canRecoverShards = true;
//        }
        }

        if(pointers.size() == 0 || (hasMissingShard && !canRecoverShards)) {
            throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FILE_SHARD_MISSING_ERROR));
        }

        if(hasMissingShard) {
            //TODO: queue_recover_shards
        }

        // set shard size to the first shard
        shardSize = pointers.get(0).getSize();
        long fileSize = pointers.stream().filter(p -> !p.isParity()).mapToLong(Pointer::getSize).sum();
        // TODO: check for replace pointer

        // downloading
        AtomicLong downloadedBytes = new AtomicLong();
        CompletableFuture[] downFutures = pointers
                .stream()
                .filter(p -> !p.isParity())
                .map(p -> {
                    CompletableFuture<ByteBuffer> fu = downloadShardByPointer(p);
                    progress.onProgress(0f, "begin downloading " + p);
                    fu.thenAcceptAsync(bf -> {
                        long thisSize = p.getSize();
                        downloadedBytes.addAndGet(thisSize);
                        try {
                            fileChannel.write(bf, shardSize * p.getIndex());
                        } catch (IOException e) {
                            throw new GenaroRuntimeException(Genaro.GenaroStrError(GENARO_FILE_WRITE_ERROR));
                        }
                        progress.onProgress(downloadedBytes.floatValue() / fileSize, "done: " + p);
                    }).exceptionally(ex -> {
//                        fu.completeExceptionally(ex.getCause());
//                        System.err.println("Error! " + e.getMessage());
//                        throw new GenaroRuntimeException(ex.getMessage());
                        logger.error(ex.getMessage());

                        // TODO: how to process

                        isDownloadError = true;

                        return null;
                    });

                    return fu;
                })
                .toArray(CompletableFuture[]::new);

        // TODO: need better error processing
        if(isDownloadError) {
            fileChannel.close();
            this.progress.onEnd(1);
            return;
        }

        CompletableFuture<Void> futureAll = CompletableFuture.allOf(downFutures);
        futureAll.get(); // all done

        fileChannel.truncate(fileSize);

        // decryption:
        progress.onProgress(1f, "download complete");
        logger.info("download complete, begin decryption");
        byte[] bucketId = Hex.decode(file.getBucket());
        byte[] index   = Hex.decode(file.getIndex());
        byte[] fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), bucketId, index);
        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);
        javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(DECRYPT_MODE, keySpec, iv);

        try (InputStream in = Channels.newInputStream(fileChannel);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            Files.copy(cypherIn, Paths.get(this.path));
        }
        fileChannel.close();
        this.progress.onEnd(0);
    }
}
