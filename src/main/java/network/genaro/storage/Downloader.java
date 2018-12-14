package network.genaro.storage;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Callback;
import okhttp3.Call;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.encoders.Hex;
import org.xbill.DNS.utils.base16;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import static javax.crypto.Cipher.DECRYPT_MODE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import network.genaro.storage.GenaroCallback.*;
import static network.genaro.storage.Parameters.*;
import static network.genaro.storage.Genaro.genaroStrError;

public class Downloader implements Runnable {
    // each shard has GENARO_DEFAULT_MIRRORS mirrors(not include the first uploaded shard) at most
    public static final int GENARO_DEFAULT_MIRRORS = 5;
    public static final int GENARO_MAX_REPORT_TRIES = 2;
    public static final int GENARO_MAX_GET_POINTERS = 3;
    public static final int GENARO_MAX_GET_FILE_INFO = 3;

    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private String path;
    private String tempPath;
    private Genaro bridge;
    private String bucketId;
    private String fileId;
    private boolean overwrite;

    private AtomicLong downloadedBytes = new AtomicLong();
    private long totalBytes;

    // increased uploaded bytes since last onProgress Call
    private AtomicLong deltaDownloaded = new AtomicLong();

    private long shardSize;

    private FileChannel downFileChannel;

    private CompletableFuture<File> futureGetFileInfo;
    private CompletableFuture<List<Pointer>> futureGetPointers;
    private CompletableFuture<Void> futureAllRequestShard;

    // the CompletableFuture that runs this Downloader
    private CompletableFuture<Void> futureBelongsTo;

    // whether cancel() is called
    private boolean isCanceled = false;
    // ensure not stop again
    private boolean isStopping = false;

    private ResolveFileCallback resolveFileCallback;

    // 使用CachedThreadPool比较耗内存，并发高的时候会造成内存溢出
    // private static final ExecutorService uploaderExecutor = Executors.newCachedThreadPool();

    // 如果是CPU密集型应用，则线程池大小建议设置为N+1，如果是IO密集型应用，则线程池大小建议设置为2N+1，下载和上传都是IO密集型。（parallelStream也能实现多线程，但是适用于CPU密集型应用）
    private final ExecutorService downloaderExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    private final OkHttpClient downHttpClient = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path, final boolean overwrite, final ResolveFileCallback resolveFileCallback) {
        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        this.path = path;
        this.overwrite = overwrite;
        this.tempPath = path + ".genarotemp";
        this.resolveFileCallback = resolveFileCallback;
    }

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path, final boolean overwrite) {
        this(bridge, bucketId, fileId, path, overwrite, new ResolveFileCallback() {});
    }

    CompletableFuture<File> getFutureGetFileInfo() {
        return futureGetFileInfo;
    }

    void setFutureGetFileInfo(CompletableFuture<File> futureGetFileInfo) {
        this.futureGetFileInfo = futureGetFileInfo;
    }

    CompletableFuture<List<Pointer>> getFutureGetPointers() {
        return futureGetPointers;
    }

    void setFutureGetPointers(CompletableFuture<List<Pointer>> futureGetPointers) {
        this.futureGetPointers = futureGetPointers;
    }

    OkHttpClient getDownHttpClient() {
        return downHttpClient;
    }

    public CompletableFuture<Void> getFutureBelongsTo() {
        return futureBelongsTo;
    }

    public void setFutureBelongsTo(CompletableFuture<Void> futureBelongsTo) {
        this.futureBelongsTo = futureBelongsTo;
    }

    private class RequestShardCallbackFuture extends CompletableFuture<Response> implements Callback {

        public RequestShardCallbackFuture(Pointer pointer) {
            this.pointer = pointer;
        }

        private Pointer pointer;
        private static final int SEGMENT_SIZE = 2 * 1024;

        private void fail() {
            logger.error(String.format("Download Pointer %d failed", pointer.getIndex()));
            downloadedBytes.addAndGet(-pointer.getDownloadedSize());
            pointer.setDownloadedSize(0);
        }

        @Override
        public void onResponse(Call call, Response response) {
            int code = response.code();

            int errorStatus = 0;
            if (code == 401 || code == 403) {
                errorStatus = GENARO_FARMER_AUTH_ERROR;
            } else if (code == 504) {
                errorStatus = GENARO_FARMER_TIMEOUT_ERROR;
            } else if(code != 200) {
                errorStatus = GENARO_FARMER_REQUEST_ERROR;
            }

            if(errorStatus != 0) {
                fail();
                super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_FILE_WRITE_ERROR)));
                return;
            }

            byte[] buff = new byte[SEGMENT_SIZE];
            int delta;
            MessageDigest downloadedMd;
            try {
                downloadedMd = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR)));
                return;
            }

            try (InputStream is = response.body().byteStream();
                 BufferedInputStream bis = new BufferedInputStream(is)) {
                while ((delta = bis.read(buff)) != -1) {
                    downloadedMd.update(buff, 0, delta);

                    downFileChannel.write(ByteBuffer.wrap(buff, 0, delta), shardSize * pointer.getIndex() + pointer.getDownloadedSize());
                    pointer.setDownloadedSize(pointer.getDownloadedSize() + delta);

                    downloadedBytes.addAndGet(delta);
                    deltaDownloaded.addAndGet(delta);

                    if (deltaDownloaded.floatValue() / totalBytes >= 0.001) {  // call onProgress every 0.1%
                       resolveFileCallback.onProgress(downloadedBytes.floatValue() / totalBytes);
                       deltaDownloaded.set(0);
                   }
                }

                byte[] prehashSha256 = downloadedMd.digest();
                byte[] prehashRipemd160 = CryptoUtil.ripemd160(prehashSha256);

                String downloadedHash = base16.toString(prehashRipemd160).toLowerCase();
                if(pointer.getDownloadedSize() != pointer.getSize() || !downloadedHash.equals(pointer.getHash())) {
                    fail();
                    super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR)));
                    return;
                }

                logger.info(String.format("Download Pointer %d finished", pointer.getIndex()));
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "requestShard") will cause an SocketException
                if (e instanceof SocketException) {
                    super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED)));
                } else {
                    super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_FARMER_REQUEST_ERROR)));
                }
                return;
            }

            super.complete(response);
        }

        @Override
        public void onFailure(Call call, IOException e) {
            super.completeExceptionally(e);
        }
    }

    // Void not void, because it will be used as a supplier of Completable.supplyAsync
    private Void requestShard(final Pointer pointer) {
        Farmer farmer = pointer.getFarmer();
        String url = String.format("http://%s:%s/shards/%s?token=%s", farmer.getAddress(), farmer.getPort(), pointer.getHash(), pointer.getToken());
        Request request = new Request.Builder()
                                     .tag("requestShard")
                                     .header("x-storj-node-id", farmer.getNodeID())
                                     .url(url)
                                     .get()
                                     .build();

        logger.info(String.format("Starting download Pointer %d...", pointer.getIndex()));

        RequestShardCallbackFuture future = new RequestShardCallbackFuture(pointer);
        downHttpClient.newCall(request).enqueue(future);

        try {
            future.get();
        } catch (Exception e) {
            if(e instanceof ExecutionException) {
                Throwable cause = e.getCause();
                if (cause instanceof SocketTimeoutException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_TIMEOUT_ERROR));
                } else if (cause instanceof GenaroRuntimeException) {
                    throw new GenaroRuntimeException(cause.getMessage());
                }
            } else {
                throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_REQUEST_ERROR));
            }
        }
        return null;
    }

    public void start() {
        if(!overwrite && Files.exists(Paths.get(path))) {
            resolveFileCallback.onFail("File already exists");
            return;
        }

        resolveFileCallback.onBegin();

        // request info
        File file = null;

        for (int i = 0; i < GENARO_MAX_GET_FILE_INFO; i++) {
            try {
                file = bridge.getFileInfo(this, bucketId, fileId);
            } catch (Exception e) {
                if (i == GENARO_MAX_GET_FILE_INFO - 1) {
                    stop();
                    if (e instanceof CancellationException) {
                        resolveFileCallback.onCancel();
                    } else if (e instanceof TimeoutException) {
                        resolveFileCallback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        resolveFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        resolveFileCallback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    }
                    return;
                }
                // try again
                continue;
            }
            // success
            break;
        }

        // check if cancel() is called
        if(isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        // request pointers
        List<Pointer> pointers = null;

        for (int i = 0; i < GENARO_MAX_GET_POINTERS; i++) {
            try {
                pointers = bridge.getPointers(this, bucketId, fileId);
            } catch (Exception e) {
                if (i == GENARO_MAX_GET_POINTERS - 1) {
                    stop();
                    if (e instanceof CancellationException) {
                        resolveFileCallback.onCancel();
                    } else if (e instanceof TimeoutException) {
                        resolveFileCallback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        resolveFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        resolveFileCallback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    }
                    return;
                }
                // try again
                continue;
            }
            // success
            break;
        }

        // check if cancel() is called
        if(isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        boolean hasMissingShard = pointers.stream().anyMatch(pointer -> pointer.isMissing());

        // TODO: isRs() is not the
//        boolean canRecoverShards = file.isRs();
        boolean canRecoverShards = false;

        if(file.isRs()) {
            int missingPointers = (int) pointers.stream().filter(pointer -> pointer.isMissing()).count();

            // TODO:
//            if(missingPointers <= total_parity_pointers) {
//                canRecoverShards = true;
//            }
        }

        if(pointers.size() == 0 || (hasMissingShard && !canRecoverShards)) {
            stop();
            resolveFileCallback.onFail(genaroStrError(GENARO_FILE_SHARD_MISSING_ERROR));
            return;
        }

        if(hasMissingShard) {
            //TODO: queue_recover_shards
        }

        // set shard size to the size of the first shard
        shardSize = pointers.get(0).getSize();

        long fileSize = 0;
        for (Pointer pointer: pointers) {
            logger.info(pointer.toBriefString());
            long size = pointer.getSize();
            totalBytes += size;
            if(!pointer.isParity()) {
                fileSize += size;
            }
        }

        // TODO: check for replace pointer

        try {
            downFileChannel = FileChannel.open(Paths.get(tempPath), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
        } catch (IOException e) {
            stop();
            resolveFileCallback.onFail("Create temp file error");
            return;
        }

        // downloading
        CompletableFuture<Void>[] downFutures = pointers
            .stream()
            // TODO: not only download none parity shards
//            .filter(pointer -> !pointer.isParity())
            .map(pointer -> CompletableFuture.supplyAsync(() -> requestShard(pointer), downloaderExecutor))
            .toArray(CompletableFuture[]::new);

        futureAllRequestShard = CompletableFuture.allOf(downFutures);

        try {
            futureAllRequestShard.get();
        } catch (Exception e) {
            stop();
            if(e instanceof CancellationException) {
                resolveFileCallback.onCancel();
            } else if(e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                resolveFileCallback.onFail(e.getCause().getMessage());
            } else {
                logger.warn("Warn: Can not get here");
                resolveFileCallback.onFail(genaroStrError(GENARO_FARMER_REQUEST_ERROR));
            }
            return;
        }

        // check if cancel() is called
        if(isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        if(downloadedBytes.get() != totalBytes) {
            logger.warn("Downloaded bytes is not the same with total bytes, downloaded bytes: " + downloadedBytes + ", totalBytes: " + totalBytes);
        }

        try {
            downFileChannel.truncate(fileSize);
        } catch (IOException e) {
            stop();
            resolveFileCallback.onFail(genaroStrError(GENARO_FILE_RESIZE_ERROR));
            return;
        }

        // decryption:
        logger.info("Download complete, begin to decrypt...");
        byte[] bucketId = Hex.decode(file.getBucket());
        byte[] index   = Hex.decode(file.getIndex());

        byte[] fileKey;
        try {
            fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), bucketId, index);
        } catch (Exception e) {
            stop();
            resolveFileCallback.onFail("Generate file key error");
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
            stop();
            resolveFileCallback.onFail("Init decryption context error");
            return;
        }

        // check if cancel() is called
        if(isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        try (InputStream in = Channels.newInputStream(downFileChannel);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            try {
                if (overwrite) {
                    Files.copy(cypherIn, Paths.get(path), StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.copy(cypherIn, Paths.get(path));
                }
            } catch (IOException e) {
                stop();
                resolveFileCallback.onFail("Create file error");
                return;
            }
        } catch (IOException e) {
            stop();
            resolveFileCallback.onFail(genaroStrError(GENARO_FILE_DECRYPTION_ERROR));
            return;
        }

        try {
            downFileChannel.close();
        } catch (Exception e) {
            // do not call resolveFileCallback.onFail here
            logger.warn("File close exception");
        }

        logger.info("Decrypt complete, download is success");

        // download success
        resolveFileCallback.onFinish();
    }

    private void stop() {
        if (isStopping) {
            return;
        }

        isStopping = true;

        // cancel getFileInfo
        if(futureGetFileInfo != null && !futureGetFileInfo.isDone()) {
            // cancel the okhttp3 transfer
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "getFileInfo");
            // will cause a CancellationException, and will be caught on bridge.getFileInfo
            futureGetFileInfo.cancel(true);
        }

        // cancel getPointers
        if(futureGetPointers != null && !futureGetPointers.isDone()) {
            // cancel the okhttp3 transfer
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "getPointersRaw");
            // will cause a CancellationException, and will be caught on bridge.getPointers
            futureGetPointers.cancel(true);
        }

        // cancel requestShard
        if(futureAllRequestShard != null && !futureAllRequestShard.isDone()) {
            // cancel the okhttp3 transfer
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "requestShard");
            // will cause a CancellationException, and will be caught on futureAllRequestShard.get
            futureAllRequestShard.cancel(true);
        }

        downloaderExecutor.shutdown();
    }

    // Non-blocking
    public void cancel() {
        isCanceled = true;
        stop();
    }

    // wait for finish
    public void join() {
        if(futureBelongsTo != null) {
            futureBelongsTo.join();
        }
    }

    @Override
    public void run() {
        start();
    }
}
