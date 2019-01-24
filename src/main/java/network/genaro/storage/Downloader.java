package network.genaro.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import static javax.crypto.Cipher.DECRYPT_MODE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.Request;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import org.bouncycastle.util.encoders.Hex;
import org.xbill.DNS.utils.base16;

import com.backblaze.erasure.OutputInputByteTableCodingLoop;
import com.backblaze.erasure.ReedSolomon;

import network.genaro.storage.GenaroCallback.ResolveFileCallback;
import static network.genaro.storage.Parameters.*;
import static network.genaro.storage.Genaro.genaroStrError;
import static network.genaro.storage.Pointer.PointerStatus.*;

public final class Downloader implements Runnable {
    // each shard has GENARO_DEFAULT_MIRRORS mirrors(not include the first uploaded shard) at most
    static final int GENARO_DEFAULT_MIRRORS = 5;
    static final int GENARO_MAX_REPORT_TRIES = 2;
    static final int GENARO_MAX_REQUEST_POINTERS = 3;
    static final int GENARO_MAX_GET_FILE_INFO = 3;

    private String path;
    private String tempPath;
    private Genaro bridge;
    private String bucketId;
    private String fileId;
    private boolean overwrite;

    // the total downloaded bytes
    private AtomicLong downloadedBytes = new AtomicLong();
    private long totalBytes;
    private long fileSize;

    // increased uploaded bytes since last onProgress Call
    private AtomicLong deltaDownloaded = new AtomicLong();

    private AtomicBoolean stopOnProgressCall = new AtomicBoolean(false);

    private GenaroFile file;
    private List<Pointer> pointers;

    private long shardSize;
    private int totalPointers;
    private int totalDataPointers;
    private int totalParityPointers;

    private FileChannel downFileChannel;

    private CompletableFuture<GenaroFile> futureGetFileInfo;
    private CompletableFuture<List<Pointer>> futureGetPointers;
    private CompletableFuture<Void> futureAllFromRequestShard;

    // the CompletableFuture that runs this Downloader
    private CompletableFuture<Void> futureBelongsTo;

    // whether cancel() is called
    private boolean isCanceled = false;
    // ensure not stop again
    private boolean isStopping = false;

    private ResolveFileCallback resolveFileCallback;

    // not try to download from these farmers
    private String excludedFarmerIds;

    // whether the shard is non-missing
    private boolean[] shardsPresent;

    // whether all data shards(ignoring parity shards) are present
    private boolean isDataShardsAllPresent = false;

    // CachedThreadPool takes up too much memory，and it will cause memory overflow when high concurrency
    // private static final ExecutorService uploaderExecutor = Executors.newCachedThreadPool();

    // for CPU bound application，set the thread pool size to N+1 is suggested; for I/O bound application, set the thread pool size to 2N+1 is suggested
    private final ExecutorService downloaderExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    private final OkHttpClient downHttpClient;

    private String keyStr;
    private String ctrStr;

    private boolean isDecrypt;

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String filePath, final boolean overwrite,
                      final boolean isDecrypt, final String keyStr, final String ctrStr, final ResolveFileCallback resolveFileCallback) throws GenaroException {
        if (bridge == null || bucketId == null || fileId == null || filePath == null || resolveFileCallback == null) {
            throw new GenaroException("Illegal arguments");
        }

        this.bridge = bridge;
        this.fileId = fileId;
        this.bucketId = bucketId;
        this.path = filePath;
        this.overwrite = overwrite;
        this.keyStr = keyStr;
        this.ctrStr = ctrStr;
        this.isDecrypt = isDecrypt;
        this.tempPath = filePath + ".genarotemp";
        this.resolveFileCallback = resolveFileCallback;

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
                .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS);

        // set proxy server
        String proxyAddr = bridge.getProxyAddr();
        int proxyPort = bridge.getProxyPort();
        if (proxyAddr != null && !proxyAddr.trim().isEmpty() && proxyPort > 0 && proxyPort <= 65535) {
            builder = builder.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyAddr, proxyPort)));
        }

        downHttpClient = builder.build();
    }

    public Downloader(final Genaro bridge, final String bucketId, final String fileId, final String path, final boolean overwrite,
                      final boolean isDecrypt, final String keyStr, final String ctrStr) throws GenaroException {
        this(bridge, bucketId, fileId, path, overwrite, isDecrypt, keyStr, ctrStr, new ResolveFileCallback() {});
    }

    public List<Pointer> getPointers() {
        return pointers;
    }

    public int getTotalDataPointers() {
        return totalDataPointers;
    }

    void setFutureGetFileInfo(CompletableFuture<GenaroFile> futureGetFileInfo) {
        this.futureGetFileInfo = futureGetFileInfo;
    }

    void setFutureGetPointers(CompletableFuture<List<Pointer>> futureGetPointers) {
        this.futureGetPointers = futureGetPointers;
    }

    OkHttpClient getDownHttpClient() {
        return downHttpClient;
    }

    void setFutureBelongsTo(CompletableFuture<Void> futureBelongsTo) {
        this.futureBelongsTo = futureBelongsTo;
    }

    boolean isCanceled() {
        return isCanceled;
    }

    private final class RequestShardCallbackFuture extends CompletableFuture<Response> implements Callback {
        RequestShardCallbackFuture(Downloader downloader, Pointer pointer) {
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
            Genaro.logger.warn(String.format("Download Pointer %d failed", pointer.getIndex()));
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

                    // calculate the download progress
                    if (!stopOnProgressCall.get() && deltaDownloaded.floatValue() / totalBytes >= 0.001f) {  // call onProgress every 0.1%
                        float progress;

                        // if rs, we will not wait for all the shards are downloaded, just download the number of "totalDataPointers",
                        // and be aware of that the last data shard is smaller than or equal to other shards.
                        if (file.isRs()) {
                            List<Pointer> pointers = downloader.getPointers();
                            Pointer lastDataPointer = pointers.get(totalDataPointers - 1);
                            int totalDataPointers = downloader.getTotalDataPointers();

                            // it will not download all shards
                            long fakeDownBytes = pointers.stream()
                                    .filter(pointer -> pointer.getIndex() != totalDataPointers - 1)
                                    .sorted((o1, o2) -> o2.getDownloadedSize() > o1.getDownloadedSize() ? 1 : -1)
                                    .limit(totalDataPointers - 1)
                                    .mapToLong(pointer -> pointer.getDownloadedSize())
                                    .sum();
                            fakeDownBytes += lastDataPointer.getDownloadedSize();

                            long fakeTotalBytes;

                            // the last data shard is sucessfully downloaded
                            if (!shardsPresent[totalDataPointers - 1]) {
                                fakeTotalBytes = shardSize * totalDataPointers;
                            } else {
                                fakeTotalBytes = fileSize;
                            }

                            if (fakeDownBytes >= fakeTotalBytes) {
                                stopOnProgressCall.set(true);
                            }

                            progress = fakeDownBytes * 1.0f / fakeTotalBytes;
                        } else {
                            long downBytes = downloadedBytes.get();

                            // we need to decrypt the file after all data are downloaded, so let the progress to be 99.9xx%
                            if (downBytes >= totalBytes) {
                                downBytes = totalBytes - 1;
                                stopOnProgressCall.set(true);
                            }

                            progress = downBytes * 1.0f / totalBytes;
                        }

                        // after the whole data are downloaded, Reed-Solomon algorithm and decryption may take a long time
                        if (progress >= 0.999f) {
                            progress = 0.999f;
                        }

                        resolveFileCallback.onProgress(progress);
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
                // check the downloaded data
                if(pointer.getDownloadedSize() != pointer.getSize() || !downloadedHash.equals(pointer.getHash())) {
                    fail(response);
                    super.completeExceptionally(new GenaroRuntimeException(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR)));
                    return;
                }

                shardsPresent[pointer.getIndex()] = true;
                pointer.getReport().setCode(GENARO_REPORT_SUCCESS);
                pointer.getReport().setMessage(GENARO_REPORT_SHARD_DOWNLOADED);

                Genaro.logger.info(String.format("Download Pointer %d finished", pointer.getIndex()));
            } catch (IOException e) {
                fail(response);
                if (downloader.isCanceled()) {
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

        pointer.setRequestCount(pointer.getRequestCount() + 1);

        Farmer farmer = pointer.getFarmer();
        String url = String.format("http://%s:%s/shards/%s?token=%s", farmer.getAddress(), farmer.getPort(), pointer.getHash(), pointer.getToken());
        Request request = new Request.Builder()
                                     .tag("requestShard")
                                     .url(url)
                                     .get()
                                     .build();

        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        Genaro.logger.info(String.format("Starting download Pointer %d...", pointer.getIndex()));

        RequestShardCallbackFuture future = new RequestShardCallbackFuture(this, pointer);
        downHttpClient.newCall(request).enqueue(future);

        pointer.getReport().setCode(GENARO_REPORT_FAILURE);
        pointer.getReport().setMessage(GENARO_REPORT_DOWNLOAD_ERROR);

        // save the starting time of downloading
        pointer.getReport().setStart(System.currentTimeMillis());

        try {
            future.get();
        } catch (Exception e) {
            pointer.setStatus(POINTER_ERROR);
            if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException &&
                    e.getCause().getMessage().equals(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR))) {
                pointer.getReport().setMessage(GENARO_REPORT_FAILED_INTEGRITY);
            }
        } finally {
            // save the ending time of downloading
            pointer.getReport().setEnd(System.currentTimeMillis());
        }

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
                        // success
                        break;
                    } else {
                        if (bodyNode.has("error")) {
                            Genaro.logger.warn(bodyNode.get("error").asText());
                        }
                    }
                } catch (IOException e) {
                    if(i == GENARO_MAX_REPORT_TRIES - 1) {
                        // failed
                        break;
                    }
                }
            }

            // set status so that this pointer can be replaced
            if (pointer.getStatus() == POINTER_ERROR) {
                pointer.setStatus(POINTER_ERROR_REPORTED);
            }
        }

        return pointer;
    }

    private Pointer requestReplacePointer(final Pointer pointer) {
        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        Pointer newPointer = pointer;
        newPointer.setReplaced(false);
        if (newPointer.getStatus() == POINTER_ERROR_REPORTED) {
            newPointer.setStatus(POINTER_MISSING);

            if (newPointer.getReplaceCount() >= GENARO_DEFAULT_MIRRORS) {
                return newPointer;
            }

            Farmer farmer = newPointer.getFarmer();
            String farmerId = null;
            if (farmer != null) {
                farmerId = farmer.getNodeID();
            }

            if (farmerId != null) {
                if (excludedFarmerIds == null) {
                    excludedFarmerIds = farmerId;
                } else if (!excludedFarmerIds.contains(farmerId)) {
                    excludedFarmerIds += "," + farmerId;
                }
            }

            Genaro.logger.info(String.format("Requesting replacement pointer at index: %d", newPointer.getIndex()));

            newPointer.setReplaceCount(newPointer.getReplaceCount() + 1);

            String queryArgs = String.format("limit=1&skip=%d&exclude=%s", newPointer.getIndex(), excludedFarmerIds);
            String url = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String path = String.format("%s?%s", url, queryArgs);
            String signature;
            try {
                signature = bridge.signRequest("GET", url, queryArgs);
            } catch (Exception e) {
                return newPointer;
            }

            String pubKey = bridge.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("requestReplacePointer")
                    .url(bridge.getBridgeUrl() + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = downHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();
                ObjectMapper om = new ObjectMapper();
                JsonNode bodyNode = om.readTree(responseBody);

                Genaro.logger.info(String.format("Finished request replace pointer %d - JSON Response: %s", newPointer.getIndex(), responseBody));

                if (code != 200) {
                    if (bodyNode.has("error")) {
                        Genaro.logger.warn(bodyNode.get("error").asText());
                    }
                    return newPointer;
                }

                List<Pointer> pointers = om.readValue(responseBody, new TypeReference<List<Pointer>>(){});
                Pointer replacedPointer = pointers.get(0);
                newPointer.setIndex(replacedPointer.getIndex());
                newPointer.setHash(replacedPointer.getHash());
                newPointer.setSize(replacedPointer.getSize());
                newPointer.setParity(replacedPointer.isParity());
                newPointer.setToken(replacedPointer.getToken());
                newPointer.setFarmer(replacedPointer.getFarmer());
                newPointer.setOperation(replacedPointer.getOperation());
                newPointer.setReport(new GenaroExchangeReport());
                if (newPointer.getToken() != null && newPointer.getFarmer() != null) {
                    newPointer.setReplaced(true);
                    newPointer.setStatus(POINTER_REPLACED);
                } else {
                    return newPointer;
                }
            } catch (IOException e) {
                if (isCanceled) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                }
            }

            // replace the pointer in pointers
            pointers.set(pointers.indexOf(pointer), newPointer);
        }

        return newPointer;
    }

    // verify if the file can be recovered.
    private void verifyRecover() {
        boolean shardMissingError = false;

        isDataShardsAllPresent = true;
        for(int i = 0; i < totalDataPointers; i++) {
            if (!shardsPresent[i]) {
                isDataShardsAllPresent = false;
            }
        }

        if (isDataShardsAllPresent) {
            return;
        }

        int missingPointers = (int)pointers.stream().filter(pointer -> pointer.getStatus() == POINTER_MISSING).count();
        int presentPointers = 0;
        for (boolean shardPresent: shardsPresent) {
            if (shardPresent) {
                presentPointers += 1;
            }
        }

        // todo: when shard size >= 2GB(shardSize >= (1L << 31), means that the file size > 16GB), Reed-Solomon algorithm can not work normally for java version of libgenaro for now
        // todo: when shard size >= 64MB(means that the file size > 512MB), may cause an OutOfMemoryError if use Reed-Solomon for java version of libgenaro for now
        if (shardSize >= (1L << 26) && file.isRs()) {
            shardMissingError = pointers.stream().filter(pointer -> pointer.getIndex() < totalDataPointers)
                    .anyMatch(pointer -> pointer.getStatus() == POINTER_MISSING);
            if (shardMissingError || pointers.size() == 0) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_SHARD_MISSING_ERROR));
            } else {
                return;
            }
        }

        if(file.isRs()) {
            // if the downloaded data is sufficient to recover the whole file, just stop downloading
            if (presentPointers >= totalDataPointers) {
                stop();
                return;
            } else if(missingPointers > totalParityPointers) {
                shardMissingError = true;
            }
        } else {
            if (missingPointers != 0) {
                shardMissingError = true;
            }
        }

        if(shardMissingError || pointers.size() == 0) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_SHARD_MISSING_ERROR));
        }
    }

    void start() {
        if (!overwrite && Files.exists(Paths.get(path))) {
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
        if (isCanceled) {
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
        if (isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        // set shard size to the size of the first shard
        shardSize = pointers.get(0).getSize();

        for (Pointer pointer : pointers) {
            Genaro.logger.info(pointer.toBriefString());
            long size = pointer.getSize();
            totalBytes += size;
            totalPointers += 1;
            if (!pointer.isParity()) {
                totalDataPointers += 1;
                fileSize += size;
            } else {
                totalParityPointers += 1;
            }
        }

        shardsPresent = new boolean[totalPointers];
        for (int i = 0; i < totalPointers; i++) {
            shardsPresent[i] = false;
        }

        try {
            downFileChannel = FileChannel.open(Paths.get(tempPath), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
        } catch (IOException e) {
            stop();
            resolveFileCallback.onFail("Create temp file error");
            return;
        }

        resolveFileCallback.onProgress(0.0f);

        // TODO: seems terrible for so many duplicate codes
        CompletableFuture<Void>[] downFutures = pointers
                .parallelStream()
                .map(pointer -> CompletableFuture.supplyAsync(() -> requestShard(pointer), downloaderExecutor))
                .map(future -> future.thenApplyAsync(this::sendExchangeReport, downloaderExecutor))
                // 1st requestReplacePointer
                .map(future -> future.thenApplyAsync(this::requestReplacePointer, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    // download replaced pointer
                    if (pointer.isReplaced()) {
                        requestShard(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    if (pointer.isReplaced()) {
                        sendExchangeReport(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                // 2nd requestReplacePointer
                .map(future -> future.thenApplyAsync(this::requestReplacePointer, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    // download replaced pointer
                    if (pointer.isReplaced()) {
                        requestShard(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    if (pointer.isReplaced()) {
                        sendExchangeReport(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                // 3rd requestReplacePointer
                .map(future -> future.thenApplyAsync(this::requestReplacePointer, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    // download replaced pointer
                    if (pointer.isReplaced()) {
                        requestShard(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    if (pointer.isReplaced()) {
                        sendExchangeReport(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                // 4th requestReplacePointer
                .map(future -> future.thenApplyAsync(this::requestReplacePointer, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    // download replaced pointer
                    if (pointer.isReplaced()) {
                        requestShard(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    if (pointer.isReplaced()) {
                        sendExchangeReport(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                // 5th requestReplacePointer, try request replace pointer for 5 times because GENARO_DEFAULT_MIRRORS = 5
                .map(future -> future.thenApplyAsync(this::requestReplacePointer, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    // download replaced pointer
                    if (pointer.isReplaced()) {
                        requestShard(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                .map(future -> future.thenApplyAsync((pointer) -> {
                    if (pointer.isReplaced()) {
                        sendExchangeReport(pointer);
                    }
                    return pointer;
                }, downloaderExecutor))
                .map(future -> future.thenAcceptAsync((pointer) -> verifyRecover(), downloaderExecutor))
                .toArray(CompletableFuture[]::new);

        futureAllFromRequestShard = CompletableFuture.allOf(downFutures);

        try {
            futureAllFromRequestShard.get();
        } catch (Exception e) {
            stop();
            if (e instanceof CancellationException) {
                if (isCanceled) {
                    resolveFileCallback.onCancel();
                    return;
                } else {
                    // do nothing(if the downloaded data is sufficient, may reach here)
                }
            } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                resolveFileCallback.onFail(e.getCause().getMessage());
                return;
            } else {
                Genaro.logger.warn("Warn: Would not get here");
                e.printStackTrace();
                resolveFileCallback.onFail(genaroStrError(GENARO_UNKNOWN_ERROR));
                return;
            }
        }

        // check if cancel() is called
        if (isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        // use Reed-Solomon algorithm to recover file
        if (!isDataShardsAllPresent && file.isRs()) {
            // set the progress directly to 100%
            if (downloadedBytes.get() != totalBytes) {
                resolveFileCallback.onProgress(1.0f);
            }

            byte[][] shards = new byte[totalPointers][];
            MappedByteBuffer[] dataBuffers = new MappedByteBuffer[totalPointers];
            for (int i = 0; i < totalPointers; i++) {
                int size = (int) pointers.get(i).getSize();

                try {
                    // the 3rd parameter is not "totalBytes", but "totalPointers * shardSize"
                    dataBuffers[i] = downFileChannel.map(FileChannel.MapMode.READ_WRITE, i * shardSize, shardSize);
                } catch (IOException e) {
                    resolveFileCallback.onFail(genaroStrError(GENARO_FILE_RECOVER_ERROR));
                    return;
                }

                // todo: when shardSize is too big(maybe 1g) it will casue an OutOfMemoryError exception
                try {
                    // here use "shardSize", not "size"
                    shards[i] = new byte[(int) shardSize];
                } catch (OutOfMemoryError e) {
                    resolveFileCallback.onFail(genaroStrError(GENARO_OUTOFMEMORY_ERROR));
                    return;
                }

                dataBuffers[i].get(shards[i], 0, size);
            }

            ReedSolomon reedSolomon = new ReedSolomon(totalDataPointers,
                    totalParityPointers, new OutputInputByteTableCodingLoop());

            try {
                reedSolomon.decodeMissing(shards, shardsPresent, 0, (int) shardSize);
            } catch (Exception e) {
                resolveFileCallback.onFail(genaroStrError(GENARO_FILE_RECOVER_ERROR));
                return;
            }

            for (int i = 0; i < totalDataPointers; i++) {
                int size = (int) pointers.get(i).getSize();
                dataBuffers[i].position(0);
                dataBuffers[i].put(shards[i], 0, size);
            }

            // Speed up GC
            for (int i = 0; i < totalPointers; i++) {
                shards[i] = null;
                dataBuffers[i] = null;
            }
        } else if (downloadedBytes.get() != totalBytes) {
            Genaro.logger.warn("Downloaded bytes is not the same with total bytes, downloaded bytes: " + downloadedBytes + ", totalBytes: " + totalBytes);
        } else {
            // do nothing
        }

        try {
            downFileChannel.truncate(fileSize);
        } catch (IOException e) {
            stop();
            resolveFileCallback.onFail(genaroStrError(GENARO_FILE_RESIZE_ERROR));
            return;
        }

        byte[] sha256;
        InputStream cypherIns = null;
        if (isDecrypt) {
            // decryption:
            Genaro.logger.info("Decrypt file...");
            byte[] bucketIdBytes = Hex.decode(bucketId);
            byte[] fileIdBytes = Hex.decode(fileId);

            // key for decryption
            byte[] keyBytes;
            // ctr for decryption
            byte[] ivBytes;

            String indexStr = file.getIndex();

            try {
                if (keyStr != null && ctrStr != null) {
                    keyBytes = base16.fromString(keyStr);
                    ivBytes = base16.fromString(ctrStr);
                } else if (indexStr != null && indexStr.length() == 64) {
                    // calculate decryption key based on index
                    byte[] index = Hex.decode(indexStr);

                    keyBytes = CryptoUtil.generateFileKey(bridge.getPrivateKey(), bucketIdBytes, index);
                    ivBytes = Arrays.copyOf(index, 16);
                } else {
                    // calculate decryption key based on file id
                    keyBytes = CryptoUtil.generateFileKey(bridge.getPrivateKey(), bucketIdBytes, fileIdBytes);
                    keyBytes = Hex.encode(keyBytes);
                    keyBytes = CryptoUtil.sha256(keyBytes);
                    ivBytes = Arrays.copyOf(CryptoUtil.ripemd160(fileId.getBytes()), 16);
                }
            } catch (Exception e) {
                stop();
                resolveFileCallback.onFail("AES file key error");
                return;
            }

            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(ivBytes);

            javax.crypto.Cipher cipher;
            try {
                cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
                cipher.init(DECRYPT_MODE, keySpec, ivSpec);
            } catch (Exception e) {
                stop();
                resolveFileCallback.onFail("Init decryption context error");
                return;
            }

            // decrypt file use key and ctr
            try {
                cypherIns = new CipherInputStream(Channels.newInputStream(downFileChannel), cipher);

                // transfer InputStream to FileChannel
                downFileChannel.transferFrom(Channels.newChannel(cypherIns), 0, fileSize);
            } catch (Exception e) {
                stop();
                resolveFileCallback.onFail(genaroStrError(GENARO_FILE_DECRYPTION_ERROR));
                return;
            }
        }

        // calc sha256 of the downloaded data
        try {
            sha256 = CryptoUtil.sha256OfFile(downFileChannel);
        } catch (Exception e) {
            stop();
            resolveFileCallback.onFail(genaroStrError(GENARO_FILE_READ_ERROR));
            return;
        }

        // check if cancel() is called
        if (isCanceled) {
            resolveFileCallback.onCancel();
            return;
        }

        FileChannel destFileChannel;
        try {
            if (overwrite) {
                destFileChannel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            } else {
                destFileChannel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE);
            }

            downFileChannel.transferTo(0, downFileChannel.size(), destFileChannel);

            if (cypherIns != null) {
                cypherIns.close();
            }

            downFileChannel.close();
            destFileChannel.close();
        } catch (IOException e) {
            stop();
            resolveFileCallback.onFail(genaroStrError(GENARO_FILE_WRITE_ERROR));
            return;
        }

        Genaro.logger.info("Decrypt file success, download is finished");

        resolveFileCallback.onProgress(1.0f);

        // download success
        resolveFileCallback.onFinish(fileSize, sha256);
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
