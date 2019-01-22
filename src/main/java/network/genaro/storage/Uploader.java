package network.genaro.storage;

import com.backblaze.erasure.OutputInputByteTableCodingLoop;
import com.backblaze.erasure.ReedSolomon;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.Request;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Mac;
import static javax.crypto.Cipher.ENCRYPT_MODE;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import org.xbill.DNS.utils.base16;

import org.bouncycastle.util.encoders.Hex;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.Parameters.*;
import static network.genaro.storage.Genaro.genaroStrError;
import static network.genaro.storage.ShardTracker.ShardStatus.*;
import network.genaro.storage.GenaroCallback.StoreFileCallback;

public final class Uploader implements Runnable {
    public static final int GENARO_MAX_REPORT_TRIES = 2;
    public static final int GENARO_MAX_PUSH_FRAME = 3;
    public static final int GENARO_MAX_CREATE_BUCKET_ENTRY = 3;
    public static final int GENARO_MAX_PUSH_SHARD = 5;
    public static final int GENARO_MAX_REQUEST_NEW_FRAME = 3;
    public static final int GENARO_MAX_VERIFY_BUCKET_ID = 3;
    public static final int GENARO_MAX_VERIFY_FILE_NAME = 3;

    private static long MAX_SHARD_SIZE = 4294967296L; // 4Gb
    private static long MIN_SHARD_SIZE = 2097152L; // 2Mb
    private static int SHARD_MULTIPLES_BACK = 4;
    private static int GENARO_SHARD_CHALLENGES = 4;

    private String originPath;
    private String fileName;
    private String encryptedFileName;
    private long originFileSize;

    // the .crypt file path
    private String cryptFilePath;
    // the .parity file path
    private String parityFilePath;

    private FileChannel cryptChannel;
    private FileChannel parityChannel;

    private Genaro bridge;
    private String bucketId;
    private File originFile;
    private boolean rs;

    private int totalDataShards;
    private int totalParityShards;
    private int totalShards;
    private long shardSize;

    // the total uploaded bytes
    private AtomicLong uploadedBytes = new AtomicLong();
    // increased uploaded bytes since last onProgress Call
    private AtomicLong deltaUploaded = new AtomicLong();
    private long totalBytes;

    private String frameId;
    private String hmacId;
    private String fileId;

    private byte[] index;
    private byte[] fileKey;

    private EncryptionInfo ei;

    // not try to upload to these farmers
    private List<String> excludedFarmerIds = new ArrayList<>();

    private CompletableFuture<Bucket> futureGetBucket;
    private CompletableFuture<Boolean> futureIsFileExists;
    private CompletableFuture<Frame> futureRequestNewFrame;
    private CompletableFuture<Void> futureAllFromPrepareFrame;

    // the CompletableFuture that runs this Uploader
    private CompletableFuture<Void> futureBelongsTo;

    // whether cancel() is called
    private boolean isCanceled = false;
    // ensure not stop again
    private boolean isStopping = false;

    private StoreFileCallback storeFileCallback;

    // CachedThreadPool takes up too much memory，and it will cause memory overflow when high concurrency
    // private static final ExecutorService uploaderExecutor = Executors.newCachedThreadPool();

    // for CPU bound application，set the thread pool size to N+1 is suggested; for I/O bound application, set the thread pool size to 2N+1 is suggested
    private final ExecutorService uploaderExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    private final OkHttpClient upHttpClient;

    public Uploader(final Genaro bridge, final boolean rs, final String filePath, final String fileName,
                    final String bucketId, final EncryptionInfo ei, final StoreFileCallback storeFileCallback) throws GenaroException {
        if (bridge == null || filePath == null || fileName == null || bucketId == null || ei == null || storeFileCallback == null) {
            throw new GenaroException("Illegal arguments");
        }

        this.bridge = bridge;
        this.rs = rs;
        this.originPath = filePath;
        this.fileName = fileName;
        this.originFile = new File(filePath);
        this.bucketId = bucketId;
        this.storeFileCallback = storeFileCallback;

        this.ei = ei;

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

        upHttpClient = builder.build();
    }

    public Uploader(final Genaro bridge, final boolean rs, final String filePath, final String fileName, final String bucketId, final EncryptionInfo ei) throws GenaroException {
        this(bridge, rs, filePath, fileName, bucketId, ei, new StoreFileCallback() {});
    }

    public boolean isCanceled() {
        return isCanceled;
    }

    void setFutureGetBucket(CompletableFuture<Bucket> futureGetBucket) {
        this.futureGetBucket = futureGetBucket;
    }

    void setFutureIsFileExists(CompletableFuture<Boolean> futureIsFileExists) {
        this.futureIsFileExists = futureIsFileExists;
    }

    void setFutureRequestNewFrame(CompletableFuture<Frame> futureRequestNewFrame) {
        this.futureRequestNewFrame = futureRequestNewFrame;
    }

    public void setFutureBelongsTo(CompletableFuture<Void> futureBelongsTo) {
        this.futureBelongsTo = futureBelongsTo;
    }

    OkHttpClient getUpHttpClient() {
        return upHttpClient;
    }

    private static long shardSize(final int hops) {
        return (long)(MIN_SHARD_SIZE * Math.pow(2, hops));
    }

    private static long determineShardSize(final long fileSize, int accumulator) {
        if (fileSize <= 0) {
            return 0;
        }

        accumulator = accumulator > 0 ? accumulator : 0;

        // Determine hops back by accumulator
        int hops = ((accumulator - SHARD_MULTIPLES_BACK) < 0 ) ? 0 : accumulator - SHARD_MULTIPLES_BACK;

        long byteMultiple = shardSize(accumulator);
        double check = (double)fileSize / byteMultiple;

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

        return determineShardSize(fileSize, ++accumulator);
    }

    private String createTmpName(final String encryptedFileName, final String extension) {
        String tmpFolder = System.getProperty("java.io.tmpdir");
        byte[] bytesEncoded;
        try {
            bytesEncoded = sha256((BasicUtil.string2Bytes(encryptedFileName)));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }

        return String.format("%s/%s%s", tmpFolder, base16.toString(bytesEncoded).toLowerCase(), extension);
    }

    private String getBucketEntryHmac(final byte[] fileKey, final List<ShardTracker> shards) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec secretKeySpec = new SecretKeySpec(fileKey, "HmacSHA512");
        Mac mac = Mac.getInstance("HmacSHA512");
        mac.init(secretKeySpec);

        for(ShardTracker shard: shards) {
            byte[] hash = base16.fromString(shard.getMeta().getHash());
            mac.update(hash);
        }

        byte[] digestRaw = mac.doFinal();
        return base16.toString(digestRaw).toLowerCase();
    }

    private boolean createEncryptedFile() {
        index = ei.getIndex();
        fileKey = ei.getKey();
        byte[] ivBytes = ei.getCtr();

        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);

        javax.crypto.Cipher cipher;
        try {
            cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(ENCRYPT_MODE, keySpec, iv);
        } catch (Exception e) {
            return false;
        }

        Genaro.logger.info("Encrypting file...");
        boolean isSuccess = true;

        try (InputStream in = new FileInputStream(originPath);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            cryptFilePath = createTmpName(encryptedFileName, ".crypt");
            if(cryptFilePath == null) {
                isSuccess = false;
            } else {
                cryptChannel = FileChannel.open(Paths.get(cryptFilePath), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
                cryptChannel.transferFrom(Channels.newChannel(cypherIn), 0, originFileSize);
            }
        } catch (Exception e) {
            isSuccess = false;
        }

        if (isSuccess) {
            Genaro.logger.info("Encrypt file success");
        } else {
            Genaro.logger.error("Encrypt file failed");
        }

        return isSuccess;
    }

    private boolean createParityFile() {
        parityFilePath = createTmpName(encryptedFileName, ".parity");
        if(parityFilePath == null) {
            return false;
        }

        Genaro.logger.info("Creating parity file...");

        try {
            parityChannel = FileChannel.open(Paths.get(parityFilePath), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);

            ReedSolomon reedSolomon = new ReedSolomon(totalDataShards,
                    totalParityShards, new OutputInputByteTableCodingLoop());

            byte[][] shards = new byte [totalShards][];

            ByteBuffer readBuffer = ByteBuffer.allocate((int)shardSize);
            for (int i = 0; i < totalDataShards; i++) {
                int readBytes = cryptChannel.read(readBuffer, shardSize * i);
                readBuffer.flip();

                // todo: when shardSize is too big(maybe 1g) it will casue an OutOfMemoryError exception
                shards[i] = new byte[(int)shardSize];
                readBuffer.get(shards[i], 0, readBytes);
                readBuffer.flip();
            }

            for (int i = totalDataShards; i < totalShards; i++) {
                // todo: when shardSize is too big(maybe 1g) it will casue an OutOfMemoryError exception
                shards[i] = new byte[(int)shardSize];
            }

            reedSolomon.encodeParity(shards, 0, (int)shardSize);

            for (int i = totalDataShards; i < totalShards; i++) {
                parityChannel.write(ByteBuffer.wrap(shards[i], 0, (int)shardSize));
            }
        } catch (Exception | OutOfMemoryError e) {
            Genaro.logger.error("Create parity file failed");
            return false;
        }

        Genaro.logger.info(String.format("Create parity file success", cryptFilePath));

        return true;
    }

    private ShardTracker prepareFrame(final ShardTracker shard) {
        ShardMeta shardMeta = shard.getMeta();
        shardMeta.setChallenges(new byte[GENARO_SHARD_CHALLENGES][]);
        shardMeta.setChallengesAsStr(new String[GENARO_SHARD_CHALLENGES]);
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
            byte[] challenge = BasicUtil.randomBuff(32);
            shardMeta.getChallenges()[i] = challenge;
            shardMeta.getChallengesAsStr()[i] = base16.toString(challenge).toLowerCase();
        }

        Genaro.logger.info(String.format("Creating frame for shard index %d...", shard.getIndex()));

        // Initialize context for sha256 of encrypted data
        MessageDigest shardHashMd;
        try {
            shardHashMd = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
        }

        // Calculate the merkle tree with challenges
        MessageDigest[] firstSha256ForLeaf = new MessageDigest[GENARO_SHARD_CHALLENGES];
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
            try {
                firstSha256ForLeaf[i] = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            firstSha256ForLeaf[i].update(shardMeta.getChallenges()[i]);
        }

        if (shard.getIndex() < totalDataShards) {
            shard.setShardChannel(cryptChannel);
        } else {
            shard.setShardChannel(parityChannel);
        }

        // Reset shard index when using parity shards
        int shardIndex = shard.getIndex();
        shardMeta.setIndex((shardIndex >= totalDataShards) ? shardIndex - totalDataShards : shardIndex);

        try {
            int readBytes;
            final int BYTES = AES_BLOCK_SIZE * 256;
            long totalRead = 0;
            long position = shardMeta.getIndex() * shardSize;

            ByteBuffer readBuffer = ByteBuffer.allocate(BYTES);
            FileChannel shardChannel = shard.getShardChannel();

            do {
                readBytes = shardChannel.read(readBuffer, position);
                readBuffer.flip();

                // end of file
                if (readBytes == -1) {
                    break;
                }

                totalRead += readBytes;
                byte[] readData = new byte[readBytes];

                readBuffer.get(readData, 0, readBytes);
                readBuffer.flip();

                shardHashMd.update(readData, 0, readBytes);

                for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
                    firstSha256ForLeaf[i].update(readData, 0, readBytes);
                }

                position += readBytes;
            } while (totalRead < shardSize);

            shardMeta.setSize(totalRead);
        } catch (IOException e) {
            if (isCanceled) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
            } else {
                throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_READ_ERROR));
            }
        }

        byte[] prehashSha256 = shardHashMd.digest();
        byte[] prehashRipemd160 = CryptoUtil.ripemd160(prehashSha256);

        String shardHash = base16.toString(prehashRipemd160).toLowerCase();
        shardMeta.setHash(shardHash);

        byte[] preleafSha256;
        byte[] preleafRipemd160;

        shardMeta.setTree(new String[GENARO_SHARD_CHALLENGES]);
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
            // finish first sha256 for leaf
            preleafSha256 = firstSha256ForLeaf[i].digest();

            // ripemd160 result of sha256
            preleafRipemd160 = CryptoUtil.ripemd160(preleafSha256);

            // sha256 and ripemd160 again
            try {
                shardMeta.getTree()[i] = CryptoUtil.ripemd160Sha256HexString(preleafRipemd160);
            } catch (NoSuchAlgorithmException e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
        }

        Genaro.logger.info(String.format("Create frame finished for shard index %d", shard.getIndex()));

        return shard;
    }

    private ShardTracker pushFrame(final ShardTracker shard) {
        if (shard.getStatus() == SHARD_PUSH_SUCCESS) {
            return shard;
        }

        ShardMeta shardMeta = shard.getMeta();

        boolean parityShard;

        parityShard = shard.getIndex() + 1 > totalDataShards;

        String[] challengesAsStr = shardMeta.getChallengesAsStr();
        String[] tree = shardMeta.getTree();

        ObjectMapper om = new ObjectMapper();
        String challengesJsonStr;
        String treeJsonStr;
        String excludeStr;
        try {
            challengesJsonStr = om.writeValueAsString(challengesAsStr);
            treeJsonStr = om.writeValueAsString(tree);
            excludeStr = om.writeValueAsString(excludedFarmerIds);
        } catch (JsonProcessingException e) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
        }
        String jsonStrBody = String.format("{\"hash\":\"%s\",\"size\":%d,\"index\":%d,\"parity\":%b," +
                        "\"challenges\":%s,\"tree\":%s,\"exclude\":%s}",
                shardMeta.getHash(), shardMeta.getSize(), shard.getIndex(), parityShard, challengesJsonStr, treeJsonStr, excludeStr);

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, jsonStrBody);
        String path = "/frames/" + frameId;

        String signature;
        try {
            signature = bridge.signRequest("PUT", path, jsonStrBody);
        } catch (NoSuchAlgorithmException e) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
        }
        String pubKey = bridge.getPublicKeyHexString();

        Request request = new Request.Builder()
                .tag("pushFrame")
                .url(bridge.getBridgeUrl() + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .put(body)
                .build();

        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        for (int i = 0; i < GENARO_MAX_PUSH_FRAME; i++) {
            Genaro.logger.info(String.format("Pushing frame for shard index %d(retry: %d) - JSON body: %s", shard.getIndex(), i, jsonStrBody));
            try {
                try (Response response = upHttpClient.newCall(request).execute()) {
                    int code = response.code();
                    String responseBody = response.body().string();

                    Genaro.logger.info(String.format("Push frame finished for shard index %d(retry: %d) - JSON Response: %s", shard.getIndex(), i, responseBody));

                    if (code == 429 || code == 420) {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_RATE_ERROR));
                    } else if (code != 200 && code != 201) {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_OFFER_ERROR));
                    }

                    FarmerPointer fp = om.readValue(responseBody, FarmerPointer.class);
                    shard.setPointer(fp);
                } catch (IOException e) {
                    if (isCanceled) {
                        // if it's canceled, do not try again
                        i = GENARO_MAX_PUSH_SHARD - 1;
                        throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                    } else if (e instanceof SocketTimeoutException) {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    }
                }
            } catch (GenaroRuntimeException e) {
                if(i == GENARO_MAX_PUSH_FRAME - 1) {
                    throw e;
                }
                // fail
                continue;
            }
            // success
            break;
        }

        return shard;
    }

    private ShardTracker pushShard(final ShardTracker shard) {
        if (shard.getStatus() == SHARD_PUSH_SUCCESS) {
            shard.setHasTriedToPush(false);
            return shard;
        }

        shard.setHasTriedToPush(true);
        shard.setPushCount(shard.getPushCount() + 1);

        ShardMeta shardMeta = shard.getMeta();

        Farmer farmer = shard.getPointer().getFarmer();
        String farmerAddress = farmer.getAddress();
        String farmerPort = farmer.getPort();
        String metaHash = shard.getMeta().getHash();
        long metaSize = shard.getMeta().getSize();
        FileChannel shardChannel = shard.getShardChannel();

        long filePosition = shardMeta.getIndex() * shardSize;
        String token = shard.getPointer().getToken();

        UploadRequestBody uploadRequestBody = new UploadRequestBody(shardChannel, filePosition, metaSize,
                "application/octet-stream; charset=utf-8", new UploadRequestBody.ProgressListener() {
            @Override
            public void transferred(long delta) {
                shard.setUploadedSize(shard.getUploadedSize() + delta);
                uploadedBytes.addAndGet(delta);
                deltaUploaded.addAndGet(delta);

                if (deltaUploaded.floatValue() / totalBytes >= 0.001f) {  // call onProgress every 0.1%
                    storeFileCallback.onProgress(uploadedBytes.floatValue() / totalBytes);
                    deltaUploaded.set(0);
                } else if (uploadedBytes.get() == totalBytes) {
                    storeFileCallback.onProgress(1.0f);
                    deltaUploaded.set(0);
                } else {
                    // do nothing
                }
            }
        });

        String url = String.format("http://%s:%s/shards/%s?token=%s", farmerAddress, farmerPort, metaHash, token);
        Request request = new Request.Builder()
                .tag("pushShard")
                .url(url)
                .post(uploadRequestBody)
                .build();

        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        shard.getReport().setCode(GENARO_REPORT_FAILURE);
        shard.getReport().setMessage(GENARO_REPORT_UPLOAD_ERROR);

        // save the starting time of downloading
        shard.getReport().setStart(System.currentTimeMillis());

        Genaro.logger.info(String.format("Transferring Shard index %d...", shard.getIndex()));
        try (Response response = upHttpClient.newCall(request).execute()) {
            int code = response.code();
            response.close();

            if (code == 200 || code == 201 || code == 304) {
                long uploaded = shard.getUploadedSize();
                long total = shard.getMeta().getSize();
                if (uploaded != total) {
                    Genaro.logger.warn(String.format("Shard index %d, uploaded bytes: %d, total bytes: %d", shard.getIndex(), uploaded, total));
                    if (shard.getPushCount() >= GENARO_MAX_PUSH_SHARD) {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR));
                    }
                    return shard;
                }
                shard.setStatus(SHARD_PUSH_SUCCESS);
            } else {
                if (shard.getPushCount() >= GENARO_MAX_PUSH_SHARD) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_REQUEST_ERROR));
                }
                return shard;
            }
        } catch (IOException e) {
            if (isCanceled) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
            } else if (e instanceof SocketTimeoutException) {
                if (shard.getPushCount() >= GENARO_MAX_PUSH_SHARD) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_TIMEOUT_ERROR));
                }
                return shard;
            } else {
                if (shard.getPushCount() >= GENARO_MAX_PUSH_SHARD) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_FARMER_REQUEST_ERROR));
                }
                return shard;
            }
        } finally {
            // save the ending time of downloading
            shard.getReport().setEnd(System.currentTimeMillis());
            if (shard.getStatus() != SHARD_PUSH_SUCCESS) {
                uploadedBytes.addAndGet(-shard.getUploadedSize());
                shard.setUploadedSize(0);

                // Add pointer to exclude for future calls
                String farmerId = shard.getPointer().getFarmer().getNodeID();
                if (!excludedFarmerIds.contains(farmerId)) {
                    excludedFarmerIds.add(farmerId);
                }

                Genaro.logger.info(String.format("Failed to transfer shard index %d", shard.getIndex()));
            } else {
                Genaro.logger.info(String.format("Successfully transferred shard index %d", shard.getIndex()));
            }
        }

        shard.getReport().setCode(GENARO_REPORT_SUCCESS);
        shard.getReport().setMessage(GENARO_REPORT_SHARD_UPLOADED);

        return shard;
    }

    private ShardTracker sendExchangeReport(final ShardTracker shard) {
        if (isCanceled) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
        }

        if (shard.getReport().getStart() > 0 && shard.getReport().getEnd() > 0) {
            String jsonStrBody = String.format("{\"dataHash\": \"%s\", \"farmerId\": \"%s\", \"exchangeStart\": \"%d\"," +
                            "\"exchangeEnd\": \"%d\", \"exchangeResultCode\": \"%d\", \"exchangeResultMessage\": \"%s\"}",
                    shard.getMeta().getHash(), shard.getPointer().getFarmer().getNodeID(), shard.getReport().getStart(),
                    shard.getReport().getEnd(), shard.getReport().getCode(), shard.getReport().getMessage());

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String path = "/reports/exchanges";

            String signature;
            try {
                signature = bridge.signRequest("POST", path, jsonStrBody);
            } catch (NoSuchAlgorithmException e) {
                return shard;
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
                try (Response response = upHttpClient.newCall(request).execute()) {
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
        }

        return shard;
    }

    private void createBucketEntry(final List<ShardTracker> shards) throws NoSuchAlgorithmException {
        try {
            hmacId = getBucketEntryHmac(fileKey, shards);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_GENERATE_HMAC_ERROR));
        }

        Genaro.logger.info(String.format("[%s] Creating bucket entry... ", fileName));

        String jsonStrBody = String.format("{\"frame\": \"%s\", \"filename\": \"%s\", \"index\": \"%s\", \"hmac\": {\"type\": \"sha512\", \"value\": \"%s\"}",
                frameId, encryptedFileName, Hex.toHexString(index), hmacId);

        byte[] rsaKey = ei.getRsaKey();
        byte[] rsaCtr = ei.getRsaCtr();
        if (rsaKey != null && rsaCtr != null) {
            jsonStrBody += String.format(", \"rsaKey\": \"%s\", \"rsaCtr\": \"%s\"", base16.toString(rsaKey).toLowerCase(), base16.toString(rsaCtr).toLowerCase());
        }

        if (rs) {
            jsonStrBody += String.format(", \"erasure\": {\"type\": \"reedsolomon\"}");
        }
        jsonStrBody += "}";

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, jsonStrBody);
        String path = "/buckets/" + bucketId + "/files";

        String signature;
        signature = bridge.signRequest("POST", path, jsonStrBody);

        String pubKey = bridge.getPublicKeyHexString();
        Request request = new Request.Builder()
                .url(bridge.getBridgeUrl() + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .post(body)
                .build();

        for (int i = 0; i < GENARO_MAX_CREATE_BUCKET_ENTRY; i++) {
            Genaro.logger.info(String.format("Create bucket entry(retry: %d) - JSON body: %s", i, jsonStrBody));
            try {
                try (Response response = upHttpClient.newCall(request).execute()) {
                    int code = response.code();
                    String responseBody = response.body().string();
                    ObjectMapper om = new ObjectMapper();
                    JsonNode bodyNode = om.readTree(responseBody);

                    Genaro.logger.info(String.format("Create bucket entry(retry: %d) - JSON Response: %s", i, responseBody));

                    if (code != 200 && code != 201) {
                        if (bodyNode.has("error")) {
                            Genaro.logger.warn(bodyNode.get("error").asText());
                        }
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    }

                    Genaro.logger.info("Successfully Added bucket entry");

                    fileId = bodyNode.get("id").asText();
                } catch (IOException e) {
                    if (isCanceled) {
                        // if it's canceled, do not try again
                        i = GENARO_MAX_CREATE_BUCKET_ENTRY - 1;
                        throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                    } else if (e instanceof SocketTimeoutException) {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    }
                }
            } catch (GenaroRuntimeException e) {
                if (i == GENARO_MAX_CREATE_BUCKET_ENTRY - 1) {
                    throw e;
                }
                // try again
                continue;
            }
            // success
            break;
        }
    }

    public void start() {
        if (originPath == null || originPath.trim().isEmpty() || !Files.exists(Paths.get(originPath))) {
            storeFileCallback.onFail("Invalid file path");
            return;
        }

        // calculate shard size and count
        originFileSize = originFile.length();

        shardSize = determineShardSize(originFileSize, 0);
        if (shardSize <= 0) {
            storeFileCallback.onFail(genaroStrError(GENARO_FILE_SIZE_ERROR));
            return;
        }

        // todo: when shard size >= 2GB(shardSize >= (1L << 31), means that the file size > 16GB) and you need to use Reed-Solomon, it is not supported for java version of libgenaro for now
        // todo: when shard size >= 64MB(means that the file size > 512MB) and you need to use Reed-Solomon, may cause an OutOfMemoryError if use Reed-Solomon for java version of libgenaro for now
        if (shardSize >= (1L << 26) && rs) {
            rs = false;
            Genaro.logger.warn(genaroStrError(GENARO_RS_FILE_SIZE_ERROR));
        }

        // when file size <= MIN_SHARD_SIZE, there is only one shard, Reed-Solomon is unnecessary
        if (originFileSize <= MIN_SHARD_SIZE) {
            rs = false;
        }

        totalDataShards = (int)Math.ceil(originFileSize * 1.0 / shardSize);
        totalParityShards = rs ? (int)Math.ceil(totalDataShards * 2.0 / 3.0) : 0;
        totalShards = totalDataShards + totalParityShards;
        totalBytes = originFileSize + totalParityShards * shardSize;

        storeFileCallback.onBegin(originFileSize);

        for (int i = 0; i < GENARO_MAX_VERIFY_BUCKET_ID; i++) {
            // verify bucket id
            try {
                bridge.getBucket(this, bucketId);
            } catch (Exception e) {
                if(i == GENARO_MAX_VERIFY_BUCKET_ID - 1) {
                    stop();
                    if (e instanceof CancellationException) {
                        storeFileCallback.onCancel();
                    } else if (e instanceof TimeoutException) {
                        storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        storeFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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
            storeFileCallback.onCancel();
            return;
        }

        // verify file name
        try {
            encryptedFileName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(fileName), bridge.getPrivateKey(), Hex.decode(bucketId));
        } catch (Exception e) {
            stop();
            storeFileCallback.onFail("Encrypt file name error");
            return;
        }

        boolean exist = false;
        for (int i = 0; i < GENARO_MAX_VERIFY_FILE_NAME; i++) {
            try {
                exist = bridge.isFileExist(this, bucketId, encryptedFileName);
            } catch (Exception e) {
                if(i == GENARO_MAX_VERIFY_BUCKET_ID - 1) {
                    stop();
                    if (e instanceof CancellationException) {
                        storeFileCallback.onCancel();
                    } else if (e instanceof TimeoutException) {
                        storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        storeFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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
            storeFileCallback.onCancel();
            return;
        }

        if (exist) {
            stop();
            storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_FILE_EXISTS));
            return;
        }

        if(!createEncryptedFile()) {
            stop();
            storeFileCallback.onFail(genaroStrError(GENARO_FILE_ENCRYPTION_ERROR));
            return;
        }

        if (rs && !createParityFile()) {
            stop();
            storeFileCallback.onFail(genaroStrError(GENARO_FILE_PARITY_ERROR));
            return;
        }

        // request frame id
        Genaro.logger.info("Request frame id");
        Frame frame = null;

        for (int i = 0; i < GENARO_MAX_REQUEST_NEW_FRAME; i++) {
            try {
                frame = bridge.requestNewFrame(this);
            } catch (Exception e) {
                if(i == GENARO_MAX_REQUEST_NEW_FRAME - 1) {
                    stop();
                    if (e instanceof CancellationException) {
                        storeFileCallback.onCancel();
                    } else if (e instanceof TimeoutException) {
                        storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        storeFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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
            storeFileCallback.onCancel();
            return;
        }

        frameId = frame.getId();

        Genaro.logger.info(String.format("Request frame id success, frame id: %s", frameId));

        List<ShardTracker> shards = new ArrayList<>(totalShards);
        for (int i = 0; i < totalShards; i++) {
            ShardTracker shard = new ShardTracker();
            shard.setIndex(i);
            shard.setPointer(new FarmerPointer());
            shard.setMeta(new ShardMeta(i));
            shard.getMeta().setParity(i + 1 > totalDataShards);
            shard.setReport(new GenaroExchangeReport());
            shards.add(shard);
        }

        storeFileCallback.onProgress(0.0f);

        // TODO: seems terrible for so many duplicate codes
        CompletableFuture<Void>[] upFutures = shards
                .parallelStream()
                .map(shard -> CompletableFuture.supplyAsync(() -> prepareFrame(shard), uploaderExecutor))
                // 1st pushFrame
                .map(future -> future.thenApplyAsync(this::pushFrame, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushShard, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::sendExchangeReport, uploaderExecutor))
                // 2nd pushFrame
                .map(future -> future.thenApplyAsync(this::pushFrame, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushShard, uploaderExecutor))
                .map(future -> future.thenApplyAsync(shard -> {
                    if (shard.getHasTriedToPush()) {
                        sendExchangeReport(shard);
                    }
                    return shard;
                }, uploaderExecutor))
                // 3rd pushFrame
                .map(future -> future.thenApplyAsync(this::pushFrame, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushShard, uploaderExecutor))
                .map(future -> future.thenApplyAsync(shard -> {
                    if (shard.getHasTriedToPush()) {
                        sendExchangeReport(shard);
                    }
                    return shard;
                }, uploaderExecutor))
                // 4th pushFrame
                .map(future -> future.thenApplyAsync(this::pushFrame, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushShard, uploaderExecutor))
                .map(future -> future.thenApplyAsync(shard -> {
                    if (shard.getHasTriedToPush()) {
                        sendExchangeReport(shard);
                    }
                    return shard;
                }, uploaderExecutor))
                // 5th pushFrame
                .map(future -> future.thenApplyAsync(this::pushFrame, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushShard, uploaderExecutor))
                .map(future -> future.thenApplyAsync(shard -> {
                    if (shard.getHasTriedToPush()) {
                        sendExchangeReport(shard);
                    }
                    return shard;
                }, uploaderExecutor))
                .toArray(CompletableFuture[]::new);

        futureAllFromPrepareFrame = CompletableFuture.allOf(upFutures);

        try {
            futureAllFromPrepareFrame.get();
        } catch (Exception e) {
            stop();
            if(e instanceof CancellationException) {
                if (isCanceled) {
                    storeFileCallback.onCancel();
                    return;
                } else {
                    // do nothing
                }
            } else if(e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                storeFileCallback.onFail(e.getCause().getMessage());
                return;
            } else {
                Genaro.logger.warn("Warn: Would not get here");
                e.printStackTrace();
                storeFileCallback.onFail(genaroStrError(GENARO_UNKNOWN_ERROR));
                return;
            }
        } finally {
            try {
                cryptChannel.close();
                if (rs) {
                    parityChannel.close();
                }
            } catch (IOException e) {
                // do nothing
            }
        }

        // check if cancel() is called
        if (isCanceled) {
            storeFileCallback.onCancel();
            return;
        }

        if (uploadedBytes.get() != totalBytes) {
            Genaro.logger.error("uploadedBytes: " + uploadedBytes + ", totalBytes: " + totalBytes);
            stop();
            storeFileCallback.onFail(genaroStrError(GENARO_FARMER_INTEGRITY_ERROR));
            return;
        }

        try {
            createBucketEntry(shards);
        } catch (Exception e) {
            stop();
            if (isCanceled) {
                storeFileCallback.onFail(genaroStrError(GENARO_TRANSFER_CANCELED));
            } else if(e instanceof GenaroRuntimeException) {
                storeFileCallback.onFail(e.getCause().getMessage());
            } else if(e instanceof NoSuchAlgorithmException) {
                storeFileCallback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
            } else if (e instanceof SocketTimeoutException) {
                storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
            } else {
                storeFileCallback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
            }
            return;
        }

        // it's not necessary, because 1.0f is already passed to onProgress
        // storeFileCallback.onProgress(1.0f);

        storeFileCallback.onFinish(fileId);
    }

    private void stop() {
        if (isStopping) {
            return;
        }

        isStopping = true;

        // cancel getBucket
        if (futureGetBucket != null && !futureGetBucket.isDone()) {
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "getBucket");

            // will cause a CancellationException, and will be caught on bridge.getBucket
            futureGetBucket.cancel(true);
        }

        // cancel isFileExists
        if (futureIsFileExists != null && !futureIsFileExists.isDone()) {
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "isFileExist");

            // will cause a CancellationException, and will be caught on bridge.isFileExists
            futureIsFileExists.cancel(true);
        }

        // cancel requestNewFrame
        if (futureRequestNewFrame != null && !futureRequestNewFrame.isDone()) {
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "requestNewFrame");

            // will cause a CancellationException, and will be caught on bridge.requestNewFrame
            futureRequestNewFrame.cancel(true);
        }

        if (futureAllFromPrepareFrame != null && !futureAllFromPrepareFrame.isDone()) {
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "pushFrame");
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "pushShard");
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "sendExchangeReport");

            // will cause a CancellationException, and will be caught on futureAllFromPrepareFrame.get()
            // this call will only terminate pushShard, prepareFrame and pushFrame will not be terminated,
            // but uploaderExecutor.shutdown() can terminate them
            futureAllFromPrepareFrame.cancel(true);
        }

        uploaderExecutor.shutdown();
    }

    // Non-blocking
    public void cancel() {
        isCanceled = true;
        stop();
    }

    // wait for finish
    public void join() {
        if (futureBelongsTo != null) {
            futureBelongsTo.join();
        }
    }

    @Override
    public void run() {
        start();
    }
}
