package network.genaro.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

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

import network.genaro.storage.GenaroCallback.ResolveFileCallback;
import static network.genaro.storage.Parameters.*;
import static network.genaro.storage.Genaro.genaroStrError;
import static network.genaro.storage.Pointer.PointerStatus.*;

public final class Downloader implements Runnable {
    // each shard has GENARO_DEFAULT_MIRRORS mirrors(not include the first uploaded shard) at most
    public static final int GENARO_DEFAULT_MIRRORS = 5;
    public static final int GENARO_MAX_REPORT_TRIES = 2;
    public static final int GENARO_MAX_REQUEST_POINTERS = 3;
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

    File file;
    List<Pointer> pointers;

    private long shardSize;
    private int totalParityPointers;

    private FileChannel downFileChannel;

    private CompletableFuture<File> futureGetFileInfo;
    private CompletableFuture<List<Pointer>> futureGetPointers;
    private CompletableFuture<Void> futureAllFromRequestShard;

    // the CompletableFuture that runs this Downloader
    private CompletableFuture<Void> futureBelongsTo;

    // whether cancel() is called
    private boolean isCanceled = false;
    // ensure not stop again
    private boolean isStopping = false;

    private ResolveFileCallback resolveFileCallback;

    private String excludedFarmerIds;

    // 使用CachedThreadPool比较耗内存，并发高的时候会造成内存溢出
    // private static final ExecutorService uploaderExecutor = Executors.newCachedThreadPool();

    // 如果是CPU密集型应用，则线程池大小建议设置为N+1，如果是IO密集型应用，则线程池大小建议设置为2N+1，下载和上传都是IO密集型。（parallelStream也能实现多线程，但是适用于CPU密集型应用）
    private final ExecutorService downloaderExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    private final OkHttpClient downHttpClient = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String filePath, final boolean overwrite, final ResolveFileCallback resolveFileCallback) {
        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        this.path = filePath;
        this.overwrite = overwrite;
        this.tempPath = filePath + ".genarotemp";
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

    public boolean isCanceled() {
        return isCanceled;
    }

    private final class RequestShardCallbackFuture extends CompletableFuture<Response> implements Callback {

        public RequestShardCallbackFuture(Downloader downloader, Pointer pointer) {
            this.downloader = downloader;
            this.pointer = pointer;
        }

        private Downloader downloader;
        private Pointer pointer;
        private static final int SEGMENT_SIZE = 2 * 1024;

        private void fail(Response response) {
            if (response != null) {
                response.close();
            }
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
                fail(response);
                super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_FILE_WRITE_ERROR)));
                return;
            }

            byte[] buff = new byte[SEGMENT_SIZE];
            int delta;
            MessageDigest downloadedMd;
            try {
                downloadedMd = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                fail(response);
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

                   if (downloader.isCanceled()) {
                       fail(response);
                       super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED)));
                       return;
                   }
                }

                byte[] prehashSha256 = downloadedMd.digest();
                byte[] prehashRipemd160 = CryptoUtil.ripemd160(prehashSha256);

                String downloadedHash = base16.toString(prehashRipemd160).toLowerCase();
                if(pointer.getDownloadedSize() != pointer.getSize() || !downloadedHash.equals(pointer.getHash())) {
                    fail(response);
                    super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR)));
                    return;
                }

                logger.info(String.format("Download Pointer %d finished", pointer.getIndex()));
            } catch (IOException e) {
                fail(response);
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "requestShard") will cause an SocketException
                if (e instanceof SocketException || e.getMessage() == "Canceled") {
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
            fail(null);
            super.completeExceptionally(e);
        }
    }

    private Pointer requestShard(final Pointer pointer) {
        if (pointer.getStatus() == POINTER_ERROR_REPORTED || pointer.getStatus() == POINTER_MISSING) {
            return pointer;
        }

        Farmer farmer = pointer.getFarmer();
        String url = String.format("http://%s:%s/shards/%s?token=%s", farmer.getAddress(), farmer.getPort(), pointer.getHash(), pointer.getToken());
        Request request = new Request.Builder()
                                     .tag("requestShard")
                                     .header("x-storj-node-id", farmer.getNodeID())
                                     .url(url)
                                     .get()
                                     .build();

        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        logger.info(String.format("Starting download Pointer %d...", pointer.getIndex()));

        RequestShardCallbackFuture future = new RequestShardCallbackFuture(this, pointer);
        downHttpClient.newCall(request).enqueue(future);

        // save the start time of downloading
        pointer.getReport().setStart(System.currentTimeMillis());

        try {
            future.get();
        } catch (Exception e) {
            pointer.setStatus(POINTER_ERROR);
            pointer.getReport().setCode(GENARO_REPORT_FAILURE);
            pointer.getReport().setMessage(GENARO_REPORT_DOWNLOAD_ERROR);

            if(e instanceof ExecutionException) {
                Throwable cause = e.getCause();
                if (cause instanceof SocketTimeoutException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_TIMEOUT_ERROR));
                } else if (cause instanceof GenaroRuntimeException) {
                    if (cause.getMessage().equals(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR))) {
                        pointer.getReport().setMessage(GENARO_REPORT_FAILED_INTEGRITY);
                    }
                    throw new GenaroRuntimeException(cause.getMessage());
                }
            } else {
                throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_REQUEST_ERROR));
            }
        } finally {
            // save the end time of downloading
            pointer.getReport().setEnd(System.currentTimeMillis());
        }

        pointer.getReport().setCode(GENARO_REPORT_SUCCESS);
        pointer.getReport().setMessage(GENARO_REPORT_SHARD_DOWNLOADED);

        return pointer;
    }

    private Pointer sendExchangeReport(final Pointer pointer) {
        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        if (pointer.getReport().getStart() > 0 && pointer.getReport().getEnd() > 0) {
            String jsonStrBody = String.format("{\"dataHash\": \"%s\", \"farmerId\": \"%s\", \"exchangeStart\": \"%d\"," +
                            "\"exchangeEnd\": \"%d\", \"exchangeResultCode\": \"%d\", \"exchangeResultMessage\": \"%s\"}",
                    pointer.getHash(), pointer.getFarmer().getNodeID(), pointer.getReport().getStart(),
                    pointer.getReport().getEnd(), pointer.getReport().getCode(), pointer.getReport().getMessage());

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String path = "/reports/exchanges";

            String signature;
            try {
                signature = bridge.signRequest("POST", path, jsonStrBody);
            } catch (NoSuchAlgorithmException e) {
                return pointer;
            }

            String pubKey = bridge.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("sendExchangeReport")
                    .url(bridge.getBridgeUrl() + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            for (int i = 0; i < GENARO_MAX_REPORT_TRIES; i++) {
                try (Response response = downHttpClient.newCall(request).execute()) {
                    int code = response.code();
                    String responseBody = response.body().string();
                    ObjectMapper om = new ObjectMapper();
                    JsonNode bodyNode = om.readTree(responseBody);

                    if (code == 201) {
                        // set status so that this pointer can be replaced
                        if (pointer.getStatus() == POINTER_ERROR) {
                            pointer.setStatus(POINTER_ERROR_REPORTED);
                        }

                        // success
                        break;
                    } else {
                        if (bodyNode.has("error")) {
                            logger.warn(bodyNode.get("error").asText());
                        }
                    }
                } catch (IOException e) {
                    if(i == GENARO_MAX_REPORT_TRIES - 1) {
                        // failed
                        break;
                    }
                }
            }
        }

        return pointer;
    }

    private Pointer requestReplacePointer(final Pointer pointer) {
        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        Pointer newPointer = pointer;
        if (newPointer.getStatus() == POINTER_ERROR_REPORTED) {
            for (int i = 0; i < GENARO_MAX_REQUEST_POINTERS; i++) {
                if (newPointer.getReplaceCount() >= GENARO_DEFAULT_MIRRORS) {
                    // TODO: need?
                    newPointer.setReplaceCount(0);
                    newPointer.setStatus(POINTER_MISSING);

                    break;
                }

                String farmerId = newPointer.getFarmer().getNodeID();
                if (excludedFarmerIds == null) {
                    excludedFarmerIds = farmerId;
                } else if (!excludedFarmerIds.contains(farmerId)) {
                    excludedFarmerIds += "," + farmerId;
                }

                logger.info(String.format("Requesting replacement pointer at index: %d", newPointer.getIndex()));

                newPointer.setReplaceCount(newPointer.getReplaceCount() + 1);

                String queryArgs = String.format("limit=1&skip=%d&exclude=%s", newPointer.getIndex(), excludedFarmerIds);
                String url = String.format("/buckets/%s/files/%s", bucketId, fileId);
                String path = String.format("%s?%s", url, queryArgs);
                String signature;
                try {
                    signature = bridge.signRequest("GET", url, queryArgs);
                } catch (Exception e) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
                }

                String pubKey = bridge.getPublicKeyHexString();
                Request request = new Request.Builder()
                        .tag("requestReplacePointer")
                        .url(bridge.getBridgeUrl() + path)
                        .header("x-signature", signature)
                        .header("x-pubkey", pubKey)
                        .get()
                        .build();

                try {
                    try (Response response = downHttpClient.newCall(request).execute()) {
                        int code = response.code();
                        String responseBody = response.body().string();
                        ObjectMapper om = new ObjectMapper();
                        JsonNode bodyNode = om.readTree(responseBody);

                        logger.info(String.format("Finished request replace pointer %d - JSON Response: %s", newPointer.getIndex(), responseBody));

                        if (code == 429 || code == 420) {
                            if (bodyNode.has("error")) {
                                logger.error(bodyNode.get("error").asText());
                            }
                            throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_RATE_ERROR));
                        } else if (code != 200) {
                            if (bodyNode.has("error")) {
                                logger.error(bodyNode.get("error").asText());
                            }
                            throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_POINTER_ERROR));
                        }

                        List<Pointer> pointers = om.readValue(responseBody, new TypeReference<List<Pointer>>(){});
                        newPointer = pointers.get(0);
                        newPointer.setReport(new GenaroExchangeReport());
                        if (newPointer.getToken() != null && newPointer.getFarmer() != null) {
                            newPointer.setStatus(POINTER_REPLACED);
                        } else {
                            newPointer.setStatus(POINTER_MISSING);
                        }
                    } catch (IOException e) {
                        if (e instanceof SocketException || e.getMessage() == "Canceled") {
                            // if it's canceled, do not try again
                            i = GENARO_MAX_REQUEST_POINTERS - 1;
                            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                        } else if (e instanceof SocketTimeoutException) {
                            throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                        } else {
                            throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                        }
                    }
                } catch (GenaroRuntimeException e) {
                    if(i == GENARO_MAX_REQUEST_POINTERS - 1) {
                        newPointer.setStatus(POINTER_MISSING);
                        break;
                    }
                    // try again
                    continue;
                }
                break;
            }

            // replace the pointer in pointers
            pointers.set(pointers.indexOf(pointer), newPointer);
        }

        return newPointer;
    }

    // verify if the file can be recovered.
    private void verifyRecover() {
        // TODO: isRs() is not the
        boolean canRecoverShards = true;

        int missingPointers = (int) pointers.stream().filter(pointer -> pointer.getStatus() == POINTER_MISSING).count();
        if(file.isRs()) {
            if(missingPointers > totalParityPointers) {
                canRecoverShards = false;
            }
        } else {
            canRecoverShards = false;
        }

        if(pointers.size() == 0 || !canRecoverShards) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_SHARD_MISSING_ERROR));
        }
    }

    public void start() {
        if(!overwrite && Files.exists(Paths.get(path))) {
            resolveFileCallback.onFail("File already exists");
            return;
        }

        resolveFileCallback.onBegin();

        // request info
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
        for (int i = 0; i < GENARO_MAX_REQUEST_POINTERS; i++) {
            try {
                pointers = bridge.requestPointers(this, bucketId, fileId);
            } catch (Exception e) {
                if (i == GENARO_MAX_REQUEST_POINTERS - 1) {
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

        // set shard size to the size of the first shard
        shardSize = pointers.get(0).getSize();

        long fileSize = 0;
        for (Pointer pointer: pointers) {
            logger.info(pointer.toBriefString());
            long size = pointer.getSize();
            totalBytes += size;
            if(!pointer.isParity()) {
                fileSize += size;
            } else {
                totalParityPointers += 1;
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
            .map(pointer -> CompletableFuture.supplyAsync(() -> requestShard(pointer), downloaderExecutor))
            .map(future -> future.thenApplyAsync(this::sendExchangeReport, downloaderExecutor))
            .map(future -> future.thenApplyAsync(this::requestReplacePointer, downloaderExecutor))
            .map(future -> future.thenApplyAsync((pointer) -> {
                // download replaced pointer
                if (pointer.getStatus() == POINTER_REPLACED) {
                    requestShard(pointer);
                }
                return null;
            }, downloaderExecutor))
            .map(future -> future.thenAcceptAsync((obj) -> verifyRecover(), downloaderExecutor))
            .toArray(CompletableFuture[]::new);

        futureAllFromRequestShard = CompletableFuture.allOf(downFutures);

        try {
            futureAllFromRequestShard.get();
        } catch (Exception e) {
            stop();
            if(e instanceof CancellationException) {
                resolveFileCallback.onCancel();
            } else if(e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                resolveFileCallback.onFail(e.getCause().getMessage());
            } else {
                logger.warn("Warn: Would not get here");
                resolveFileCallback.onFail(genaroStrError(GENARO_UNKNOWN_ERROR));
            }
            return;
        }

        // check if cancel() is called
        if(isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

//        if(hasMissingShard) {
//            //TODO: queue_recover_shards
//        }

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

        // cancel requestPointers
        if(futureGetPointers != null && !futureGetPointers.isDone()) {
            // cancel the okhttp3 transfer
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "requestPointersRaw");

            // will cause a CancellationException, and will be caught on bridge.requestPointers
            futureGetPointers.cancel(true);
        }

        // cancel requestShard
        if(futureAllFromRequestShard != null && !futureAllFromRequestShard.isDone()) {
            // cancel the okhttp3 transfer
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "requestShard");
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "sendExchangeReport");
            BasicUtil.cancelOkHttpCallWithTag(downHttpClient, "requestReplacePointer");

            // will cause a CancellationException, and will be caught on futureAllFromRequestShard.get
            futureAllFromRequestShard.cancel(true);
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
