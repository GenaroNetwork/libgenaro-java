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

import static network.genaro.storage.Genaro.GenaroStrError;

interface DownloadProgress {
    default void onBegin() { System.out.println("Download started"); }

    default void onFinish(String error) {
        if(error != null) {
            System.out.println("Download failed: " + error);
        } else {
            System.out.println("Download finished");
        }
    }

    /**
     * called when progress update
     * @param progress range from 0 to 1
     */
    default void onProgress(float progress) { }
}

public class Downloader {
    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private String path;
    private String tempPath;
    private Genaro bridge;
    private String bucketId;
    private String fileId;
    private DownloadProgress progress;

    private long shardSize;
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    private boolean isDownloadError = false;
    private String errorMsg;

    // 使用CachedThreadPool比较耗内存，并发200+的时候会造成内存溢出
    // private static final ExecutorService downloaderExecutor = Executors.newCachedThreadPool();

    // 如果是CPU密集型应用，则线程池大小建议设置为N+1，如果是IO密集型应用，则线程池大小建议设置为2N+1，下载和上传都是IO密集型。（parallelStream也能实现多线程，但是适用于CPU密集型应用）
    private static final ExecutorService downloaderExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path, final DownloadProgress progress) {
        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        this.path = path;
        this.tempPath = path + ".genarotemp";
        this.progress = progress;
    }

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path) {
        this(bridge, bucketId, fileId, path, new DownloadProgress() {});
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
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_AUTH_ERROR));
                        case 504:
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_TIMEOUT_ERROR));
                        default:
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_REQUEST_ERROR));
                    }
                }
                byte[] body = response.body().bytes();
                return ByteBuffer.wrap(body);
            }
        }, downloaderExecutor);
    }
    public void start() {
//    public void start() throws GenaroRuntimeException, IOException, TimeoutException, ExecutionException, InterruptedException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        progress.onBegin();

        isDownloadError = false;

        // request info
        File file;
        try {
            file = bridge.getFileInfo(bucketId, fileId);
        } catch (Exception e) {
            progress.onFinish(e.getMessage());
            return;
        }

        // request pointers
        List<Pointer> pointers;
        try {
            pointers = bridge.getPointers(bucketId, fileId);
        } catch (Exception e) {
            progress.onFinish(e.getMessage());
            return;
        }

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
            progress.onFinish(GenaroStrError(GENARO_FILE_SHARD_MISSING_ERROR));
            return;
        }

        if(hasMissingShard) {
            //TODO: queue_recover_shards
        }

        // set shard size to the first shard
        shardSize = pointers.get(0).getSize();
        long fileSize = pointers.stream().filter(p -> !p.isParity()).mapToLong(Pointer::getSize).sum();

        // TODO: check for replace pointer

        FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(Paths.get(tempPath), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
        } catch (IOException e) {
            progress.onFinish("Create temp file error!");
            return;
        }

        // downloading
        AtomicLong downloadedBytes = new AtomicLong();
        CompletableFuture[] downFutures = pointers
            .stream()
            // TODO:
            .filter(p -> !p.isParity())
            .map(p -> {
                CompletableFuture<ByteBuffer> fu = downloadShardByPointer(p);
                fu.thenAcceptAsync(bf -> {
                    long thisSize = p.getSize();
                    downloadedBytes.addAndGet(thisSize);
                    try {
                        fileChannel.write(bf, shardSize * p.getIndex());
                    } catch (IOException e) {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_WRITE_ERROR));
                    }
                    progress.onProgress(downloadedBytes.floatValue() / fileSize);
                }).exceptionally(ex -> {
//                        fu.completeExceptionally(ex.getCause());
//                        System.err.println("Error! " + e.getMessage());
//                        throw new GenaroRuntimeException(ex.getMessage());
                    logger.error(ex.getMessage());

                    // TODO: how to process

                    isDownloadError = true;
                    errorMsg = ex.getMessage();

                    return null;
                });

                return fu;
            })
            .toArray(CompletableFuture[]::new);

        // TODO: need better error processing
        if(isDownloadError) {
            try { fileChannel.close(); } catch(IOException e) { }
            progress.onFinish(errorMsg);
            return;
        }

        CompletableFuture<Void> futureAll = CompletableFuture.allOf(downFutures);

        try {
            futureAll.get();
        } catch (Exception e) {
            progress.onFinish(e.getCause().getMessage());
            return;
        }

        try {
            fileChannel.truncate(fileSize);
        } catch (IOException e) {
            progress.onFinish(GenaroStrError(GENARO_FILE_RESIZE_ERROR));
            return;
        }

        // decryption:
        progress.onProgress(1.0f);
        logger.info("Download complete, begin to decrypt...");
        byte[] bucketId = Hex.decode(file.getBucket());
        byte[] index   = Hex.decode(file.getIndex());

        byte[] fileKey;
        try {
            fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), bucketId, index);
        } catch (Exception e) {
            progress.onFinish("Generate file key error!");
            return;
        }

        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);

        javax.crypto.Cipher cipher;
        try {
            cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(DECRYPT_MODE, keySpec, iv);
        } catch (Exception e) {
            progress.onFinish("Init decryption context error!");
            return;
        }

        try (InputStream in = Channels.newInputStream(fileChannel);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            try {
                Files.copy(cypherIn, Paths.get(path));
            } catch (IOException e) {
                progress.onFinish("Create file error!");
                return;
            }
        } catch (IOException e) {
            progress.onFinish("File decryption error!");
            return;
        }

        try {
            fileChannel.close();
        } catch (Exception e) {
            logger.warn("File close exception");
        }

        progress.onFinish(null);

        logger.info("Decrypt complete, download is success");
    }
}
