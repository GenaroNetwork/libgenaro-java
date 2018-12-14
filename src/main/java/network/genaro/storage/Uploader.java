package network.genaro.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bouncycastle.util.encoders.Hex;

import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.*;
import java.io.File;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static javax.crypto.Cipher.ENCRYPT_MODE;

import org.xbill.DNS.utils.base16;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.Parameters.*;

import javax.crypto.Mac;

import static network.genaro.storage.Genaro.GenaroStrError;

public class Uploader implements Runnable {
    public static final int GENARO_MAX_REPORT_TRIES = 2;
    public static final int GENARO_MAX_PUSH_FRAME = 3;
    public static final int GENARO_MAX_CREATE_BUCKET_ENTRY = 3;
    public static final int GENARO_MAX_PUSH_SHARD = 3;
    public static final int GENARO_MAX_REQUEST_NEW_FRAME = 3;
    public static final int GENARO_MAX_VERIFY_BUCKET_ID = 3;
    public static final int GENARO_MAX_VERIFY_FILE_NAME = 3;

    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private static Random random = new Random();
    private static long MAX_SHARD_SIZE = 4294967296L; // 4Gb
    private static long MIN_SHARD_SIZE = 2097152L; // 2Mb
    private static int SHARD_MULTIPLES_BACK = 4;
    private static int GENARO_SHARD_CHALLENGES = 4;

    private String originPath;
    private String fileName;
    private String encryptedFileName;

    // the .crypt file path
    private String cryptFilePath;

    private Genaro bridge;
    private String bucketId;
    private File originFile;
    private boolean rs;

    private int totalDataShards;
    private int totalParityShards;
    private int totalShards;

    private long shardSize;

    private String frameId;

    private byte[] index;
    private byte[] fileKey;

    private String hmacId;

    private int errorStatus;

    private String fileId;

    private AtomicLong uploadedBytes = new AtomicLong();
    private long totalBytes;

    // increased uploaded bytes since last onProgress Call
    private AtomicLong deltaUploaded = new AtomicLong();

    private CompletableFuture<Bucket> futureGetBucket;
    private CompletableFuture<Boolean> futureIsFileExists;
    private CompletableFuture<Frame> futureRequestNewFrame;
    private CompletableFuture<Void> futureAllFromPrepareFrame;
    private CompletableFuture<Void> futureCreateBucketEntry;

    // the CompletableFuture that runs this Uploader
    private CompletableFuture<Void> futureBelongsTo;

    // whether cancel() is called
    private boolean isCanceled = false;
    // ensure not stop again
    private boolean isStopping = false;

    private StoreFileCallback storeFileCallback;

    // 使用CachedThreadPool比较耗内存，并发高的时候会造成内存溢出
    // private static final ExecutorService uploaderExecutor = Executors.newCachedThreadPool();

    // 如果是CPU密集型应用，则线程池大小建议设置为N+1，如果是IO密集型应用，则线程池大小建议设置为2N+1，下载和上传都是IO密集型。（parallelStream也能实现多线程，但是适用于CPU密集型应用）
    private final ExecutorService uploaderExecutor = Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors() + 1);

    private final OkHttpClient upHttpClient = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    public Uploader(final Genaro bridge, final boolean rs, final String filePath, final String fileName, final String bucketId, final StoreFileCallback storeFileCallback) {
        this.bridge = bridge;
        this.rs = rs;
        this.originPath = filePath;
        this.fileName = fileName;
        this.originFile = new File(filePath);
        this.bucketId = bucketId;
        this.storeFileCallback = storeFileCallback;
    }

    CompletableFuture<Bucket> getFutureGetBucket() {
        return futureGetBucket;
    }

    void setFutureGetBucket(CompletableFuture<Bucket> futureGetBucket) {
        this.futureGetBucket = futureGetBucket;
    }

    CompletableFuture<Boolean> getFutureIsFileExists() {
        return futureIsFileExists;
    }

    void setFutureIsFileExists(CompletableFuture<Boolean> futureIsFileExists) {
        this.futureIsFileExists = futureIsFileExists;
    }

    CompletableFuture<Frame> getFutureRequestNewFrame() {
        return futureRequestNewFrame;
    }

    void setFutureRequestNewFrame(CompletableFuture<Frame> futureRequestNewFrame) {
        this.futureRequestNewFrame = futureRequestNewFrame;
    }

    CompletableFuture<Void> getFutureCreateBucketEntry() {
        return futureCreateBucketEntry;
    }

    public CompletableFuture<Void> getFutureBelongsTo() {
        return futureBelongsTo;
    }

    public void setFutureBelongsTo(CompletableFuture<Void> futureBelongsTo) {
        this.futureBelongsTo = futureBelongsTo;
    }

    void setFutureCreateBucketEntry(CompletableFuture<Void> futureCreateBucketEntry) {
        this.futureCreateBucketEntry = futureCreateBucketEntry;
    }

    OkHttpClient getUpHttpClient() {
        return upHttpClient;
    }

    private static long shardSize(final int hops) {
        return (long) (MIN_SHARD_SIZE * Math.pow(2, hops));
    }

    private static long determineShardSize(final long fileSize, int accumulator) {
        if (fileSize <= 0) {
            return 0;
        }

        accumulator = accumulator > 0 ? accumulator : 0;

        // Determine hops back by accumulator
        int hops = ((accumulator - SHARD_MULTIPLES_BACK) < 0 ) ? 0 : accumulator - SHARD_MULTIPLES_BACK;

        long byteMultiple = shardSize(accumulator);
        double check = (double) fileSize / byteMultiple;

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

    private static byte[] randomBuff(final int len) {
        byte[] buff = new byte[len];
        random.nextBytes(buff);
        return buff;
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
        index = randomBuff(32);
//        index = Hex.decode("1ffb37c2ac31231363a5996215e840ab75fc288f98ea77d9bee62b87f6e5852f");
        try {
            fileKey = CryptoUtil.generateFileKey(bridge.getPrivateKey(), Hex.decode(bucketId), index);
        } catch (NoSuchAlgorithmException e) {
            return false;
        }

        byte[] ivBytes = Arrays.copyOf(index, 16);
        SecretKeySpec keySpec = new SecretKeySpec(fileKey, "AES");
        IvParameterSpec iv = new IvParameterSpec(ivBytes);

        javax.crypto.Cipher cipher;
        try {
            cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(ENCRYPT_MODE, keySpec, iv);
        } catch (Exception e) {
            return false;
        }

        try (InputStream in = new FileInputStream(originPath);
             InputStream cypherIn = new CipherInputStream(in, cipher)) {
            cryptFilePath = createTmpName(encryptedFileName, ".crypt");
            if(cryptFilePath == null) {
                return false;
            }
            Files.copy(cypherIn, Paths.get(cryptFilePath), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    private ShardTracker prepareFrame(final ShardTracker shard) {
        ShardMeta shardMeta = shard.getMeta();
        shardMeta.setChallenges(new byte[GENARO_SHARD_CHALLENGES][]);
        shardMeta.setChallengesAsStr(new String[GENARO_SHARD_CHALLENGES]);
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
            byte[] challenge = randomBuff(32);
            //                        byte[] challenge ={99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
            //                                99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
            //                                99, 99, 99, 99, 99, 99, 99, 99, 99, 99,
            //                                99, 99};
            shardMeta.getChallenges()[i] = challenge;
            shardMeta.getChallengesAsStr()[i] = base16.toString(challenge).toLowerCase();
        }

        logger.info(String.format("Creating frame for shard index %d...", shard.getIndex()));

        // Initialize context for sha256 of encrypted data
        MessageDigest shardHashMd;
        try {
            shardHashMd = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_ALGORITHM_ERROR));
        }

        // Calculate the merkle tree with challenges
        MessageDigest[] firstSha256ForLeaf = new MessageDigest[GENARO_SHARD_CHALLENGES];
        for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
            try {
                firstSha256ForLeaf[i] = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_ALGORITHM_ERROR));
            }
            firstSha256ForLeaf[i].update(shardMeta.getChallenges()[i]);
        }

        if (shard.getIndex() + 1 > totalDataShards) {
            // TODO: Reed-solomn is not implemented yet
            shard.setShardFile("xxxxxx");
        } else {
            shard.setShardFile(cryptFilePath);
        }

        // Reset shard index when using parity shards
        int shardIndex = shard.getIndex();
        shardMeta.setIndex((shardIndex + 1 > totalDataShards) ? shardIndex - totalDataShards : shardIndex);

        try (FileInputStream fin = new FileInputStream(cryptFilePath)) {
            int readBytes;
            final int BYTES = AES_BLOCK_SIZE * 256;

            long totalRead = 0;

            long position = shardMeta.getIndex() * shardSize;

            byte[] readData = new byte[BYTES];
            do {
                // set file position
                fin.getChannel().position(position);

                readBytes = fin.read(readData, 0, BYTES);

                // end of file
                if (readBytes == -1) {
                    break;
                }

                totalRead += readBytes;
                position += readBytes;

                shardHashMd.update(readData, 0, readBytes);

                for (int i = 0; i < GENARO_SHARD_CHALLENGES; i++) {
                    firstSha256ForLeaf[i].update(readData, 0, readBytes);
                }
            } while (totalRead < shardSize);

            shardMeta.setSize(totalRead);
        } catch (IOException e) {
            if (e instanceof ClosedByInterruptException) {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
            } else {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_READ_ERROR));
            }
        }

        byte[] prehashSha256 = shardHashMd.digest();
        byte[] prehashRipemd160 = CryptoUtil.ripemd160(prehashSha256);

        shardMeta.setHash(base16.toString(prehashRipemd160).toLowerCase());

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
                throw new GenaroRuntimeException(GenaroStrError(GENARO_ALGORITHM_ERROR));
            }
        }

        logger.info(String.format("Create frame finished for shard index %d", shard.getIndex()));

        return shard;
    }

    private ShardTracker pushFrame(final ShardTracker shard) {
        //        if(shard.getPointer().getToken() == null) {
        //          logger.error("Token error");
        //          System.exit(1);
        //        }

        ShardMeta shardMeta = shard.getMeta();

        boolean parityShard;

        parityShard = shard.getIndex() + 1 > totalDataShards;

        String[] challengesAsStr = shardMeta.getChallengesAsStr();
        String[] tree = shardMeta.getTree();

        // TODO: exclude is empty for now
        ObjectMapper om = new ObjectMapper();
        String challengesJsonStr;
        String treeJsonStr;
        try {
            challengesJsonStr = om.writeValueAsString(challengesAsStr);
            treeJsonStr = om.writeValueAsString(tree);
        } catch (JsonProcessingException e) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_ALGORITHM_ERROR));
        }
        String jsonStrBody = String.format("{\"hash\":\"%s\",\"size\":%d,\"index\":%d,\"parity\":%b," +
                        "\"challenges\":%s,\"tree\":%s,\"exclude\":[]}",
                shardMeta.getHash(), shardMeta.getSize(), shard.getIndex(), parityShard, challengesJsonStr, treeJsonStr);

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, jsonStrBody);
        String path = "/frames/" + frameId;

        String signature;
        try {
            signature = bridge.signRequest("PUT", path, jsonStrBody);
        } catch (NoSuchAlgorithmException e) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_ALGORITHM_ERROR));
        }
        String pubKey = bridge.getPublicKeyHexString();
        Request request = new Request.Builder()
                .tag("pushFrame")
                .url(bridge.getBridgeUrl() + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .put(body)
                .build();

        for (int i = 0; i < GENARO_MAX_PUSH_FRAME; i++) {
            logger.info(String.format("Pushing frame for shard index %d(retry: %d) - JSON body: %s", shard.getIndex(), i, jsonStrBody));
            try {
                try (Response response = upHttpClient.newCall(request).execute()) {
                    String responseBody = response.body().string();

                    logger.info(String.format("Push frame finished for shard index %d(retry: %d) - JSON Response: %s", shard.getIndex(), i, responseBody));

                    int code = response.code();

                    if (code == 429 || code == 420) {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_RATE_ERROR));
                    } else if (code != 200 && code != 201) {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_OFFER_ERROR));
                    }

                    FarmerPointer fp = om.readValue(responseBody, FarmerPointer.class);
                    shard.setPointer(fp);
                } catch (IOException e) {
                    // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "pushFrame") will cause an SocketException
                    if (e instanceof SocketException) {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                    } else {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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
        //                  // Reset shard index when using parity shards
//                  req->shard_index = (index + 1 > state->total_data_shards) ? index - state->total_data_shards: index;
//
//                  // make sure we switch between parity and data shards files.
//                  // When using Reed solomon must also read from encrypted file
//                  // rather than the original file for the data
//                  if (index + 1 > state->total_data_shards) {
//                      req->shard_file = state->parity_file;
//                  } else if (state->rs) {
//                      req->shard_file = state->encrypted_file;
//                  } else {
//                      req->shard_file = state->original_file;
//                  }

        ShardMeta shardMeta = shard.getMeta();

        Farmer farmer = shard.getPointer().getFarmer();
        String farmerNodeId = farmer.getNodeID();
        String farmerAddress = farmer.getAddress();
        String farmerPort = farmer.getPort();
        String metaHash = shard.getMeta().getHash();
        long metaSize = shard.getMeta().getSize();
        String shardFileStr = shard.getShardFile();

        long filePosition = shardMeta.getIndex() * shardSize;
        String token = shard.getPointer().getToken();

        File shardFile = new File(shardFileStr);
        final byte[] mBlock;
        try {
            mBlock = FileUtils.getBlock(filePosition, shardFile, (int)metaSize);
        } catch (IOException e) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_READ_ERROR));
        }

        if (mBlock == null) {
            throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_READ_ERROR));
        }

//        RequestBody requestBody = RequestBody.create(MediaType.parse("application/octet-stream; charset=utf-8"), mBlock);
        UploadRequestBody uploadRequestBody = new UploadRequestBody(new ByteArrayInputStream(mBlock),
                "application/octet-stream; charset=utf-8", new UploadRequestBody.ProgressListener() {
            @Override
            public void transferred(long delta) {
                shard.setUploadedSize(shard.getUploadedSize() + delta);
                uploadedBytes.addAndGet(delta);
                deltaUploaded.addAndGet(delta);

                if (deltaUploaded.floatValue() / totalBytes >= 0.001) {  // call onProgress every 0.1%
                    storeFileCallback.onProgress(uploadedBytes.floatValue() / totalBytes);
                    deltaUploaded.set(0);
                }
            }
        });

        String url = String.format("http://%s:%s/shards/%s?token=%s", farmerAddress, farmerPort, metaHash, token);
//        String url = String.format("http://192.168.50.206:9999/");
        Request request = new Request.Builder()
                .tag("pushShard")
                .url(url)
                .header("x-storj-node-id", farmerNodeId)
                .post(uploadRequestBody)
                .build();

        for (int i = 0; i < GENARO_MAX_PUSH_SHARD; i++) {
            logger.info(String.format("Transferring Shard index %d...(retry: %d)", shard.getIndex(), i));
            try {
                try (Response response = upHttpClient.newCall(request).execute()) {
                    int code = response.code();

                    if (code == 200 || code == 201 || code == 304) {
                        long uploaded = shard.getUploadedSize();
                        long total = shard.getMeta().getSize();
                        if (uploaded != total) {
                            logger.error("Shard index %d, uploaded bytes: %d, total bytes: %d", shard.getIndex(), uploaded, total);
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_INTEGRITY_ERROR));
                        }
                        logger.info(String.format("Successfully transferred shard index %d", shard.getIndex()));
                    } else {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_REQUEST_ERROR));
                    }
                } catch (IOException e) {
                    uploadedBytes.addAndGet(-shard.getUploadedSize());
                    shard.setUploadedSize(0);
                    // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "pushShard") will cause an SocketException
                    if (e instanceof SocketException) {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                    } else {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_FARMER_REQUEST_ERROR));
                    }
                }
            } catch (GenaroRuntimeException e) {
                if (i == GENARO_MAX_PUSH_SHARD - 1) {
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

    private CompletableFuture<Void> createBucketEntryFuture(final List<ShardTracker> shards) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                hmacId = getBucketEntryHmac(fileKey, shards);
            } catch (NoSuchAlgorithmException | InvalidKeyException e) {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_GENERATE_HMAC_ERROR));
            }

            logger.info(String.format("[%s] Creating bucket entry... ", fileName));

            String jsonStrBody;
            if (!rs) {
                jsonStrBody = String.format("{\"frame\": \"%s\", \"filename\": \"%s\", \"index\": \"%s\", \"hmac\": {\"type\": \"sha512\", \"value\": \"%s\"}}",
                        frameId, encryptedFileName, Hex.toHexString(index), hmacId);
            } else {
                // TODO: ReedSolomn is not completed.
                jsonStrBody = "";
                //                  if (state->rs) {
                //                      struct json_object *erasure = json_object_new_object();
                //                      json_object *erasure_type = json_object_new_string("reedsolomon");
                //                      json_object_object_add(erasure, "type", erasure_type);
                //                      json_object_object_add(body, "erasure", erasure);
                //                  }
            }

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String path = "/buckets/" + bucketId + "/files";

            String signature;
            try {
                signature = bridge.signRequest("POST", path, jsonStrBody);
            } catch (NoSuchAlgorithmException e) {
                throw new GenaroRuntimeException(GenaroStrError(GENARO_ALGORITHM_ERROR));
            }

            String pubKey = bridge.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("createBucketEntry")
                    .url(bridge.getBridgeUrl() + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            for (int i = 0; i < GENARO_MAX_CREATE_BUCKET_ENTRY; i++) {
                try {
                    logger.info(String.format("Create bucket entry(retry: %d) - JSON body: %s", i, jsonStrBody));
                    try (Response response = upHttpClient.newCall(request).execute()) {
                        ObjectMapper om = new ObjectMapper();
                        String responseBody = response.body().string();
                        JsonNode bodyNode = om.readTree(responseBody);

                        logger.info(String.format("Create bucket entry(retry: %d) - JSON Response: %s", i, responseBody));

                        int code = response.code();

                        if (code != 200 && code != 201) {
                            String error = bodyNode.get("error").asText();
                            logger.error(error);
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                        }

                        logger.info("Successfully Added bucket entry");

                        fileId = bodyNode.get("id").asText();
                    } catch (IOException e) {
                        // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "createBucketEntry") will cause an SocketException
                        if (e instanceof SocketException) {
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                        } else {
                            throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                        }
                    }
                } catch (GenaroRuntimeException e) {
                    if(i == GENARO_MAX_CREATE_BUCKET_ENTRY - 1) {
                        throw e;
                    }
                    // fail
                    break;
                }
                // success
                break;
            }

            return null;
        });
    }

    private void createBucketEntry(final Uploader uploader, final List<ShardTracker> shards) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Void> fu = createBucketEntryFuture(shards);
        if (uploader != null) {
            uploader.setFutureCreateBucketEntry(fu);
        }
        fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public void start() {
        if (!Files.exists(Paths.get(originPath))) {
            storeFileCallback.onFail("Invalid file path");
            return;
        }

        // calculate shard size and count
        long fileSize = originFile.length();
        shardSize = determineShardSize(fileSize, 0);
        if (shardSize <= 0) {
            errorStatus = GENARO_FILE_SIZE_ERROR;
            storeFileCallback.onFail(GenaroStrError(errorStatus));
            return;
        }

        storeFileCallback.onBegin(fileSize);

        totalDataShards = (int)Math.ceil((double)fileSize / shardSize);
        totalParityShards = rs ? (int)Math.ceil((double)totalDataShards * 2.0 / 3.0) : 0;
        totalShards = totalDataShards + totalParityShards;
        totalBytes = fileSize + totalParityShards * shardSize;

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
                        storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        storeFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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
            storeFileCallback.onFail("Encrypt error");
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
                        storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        storeFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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
            storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_BUCKET_FILE_EXISTS));
            return;
        }

        if(!createEncryptedFile()) {
            stop();
            storeFileCallback.onFail(GenaroStrError(GENARO_FILE_ENCRYPTION_ERROR));
            return;
        }

        // request frame id
        logger.info("Request frame id");
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
                        storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                    } else if (e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                        storeFileCallback.onFail(e.getCause().getMessage());
                    } else {
                        storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
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

        logger.info(String.format("Request frame id success, frame id: %s", frameId));

        List<ShardTracker> shards = new ArrayList<>(totalShards);
        for (int i = 0; i < totalShards; i++) {
            ShardTracker shard = new ShardTracker();
            shard.setIndex(i);
            shard.setPointer(new FarmerPointer());
            shard.setMeta(new ShardMeta(i));
            shard.getMeta().setParity(i + 1 > totalDataShards);
            //TODO: no effect yet
            shard.setUploadedSize(0);
            shards.add(shard);
        }

        storeFileCallback.onProgress(0.0f);

        CompletableFuture[] upFutures = shards
                .stream()
                .map(shard -> CompletableFuture.supplyAsync(() -> prepareFrame(shard), uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushFrame, uploaderExecutor))
                .map(future -> future.thenApplyAsync(this::pushShard, uploaderExecutor))
                .toArray(CompletableFuture[]::new);

        futureAllFromPrepareFrame = CompletableFuture.allOf(upFutures);

        try {
            futureAllFromPrepareFrame.get();
        } catch (Exception e) {
            stop();
            if(e instanceof CancellationException) {
                storeFileCallback.onCancel();
            } else if(e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                storeFileCallback.onFail(e.getCause().getMessage());
            } else {
                storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
            }
            return;
        }

        // check if cancel() is called
        if (isCanceled) {
            storeFileCallback.onCancel();
            return;
        }

        if (uploadedBytes.get() != totalBytes) {
            logger.error("uploadedBytes: " + uploadedBytes + ", totalBytes: " + totalBytes);
            stop();
            storeFileCallback.onFail(GenaroStrError(GENARO_FARMER_INTEGRITY_ERROR));
            return;
        }

        try {
            createBucketEntry(this, shards);
        } catch (Exception e) {
            stop();
            if(e instanceof CancellationException) {
                storeFileCallback.onCancel();
            } else if(e instanceof TimeoutException) {
                storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
            } else if(e instanceof ExecutionException && e.getCause() instanceof GenaroRuntimeException) {
                storeFileCallback.onFail(e.getCause().getMessage());
            } else {
                storeFileCallback.onFail(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
            }
            return;
        }

        storeFileCallback.onProgress(1.0f);
        storeFileCallback.onFinish(fileId);

        // TODO: send exchange report
        //
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
            // will cause a CancellationException, and will be caught on futureAllFromPrepareFrame.get()
            // this call will only terminate pushShard, prepareFrame and pushFrame will not be terminated,
            // but uploaderExecutor.shutdown() can terminate them
            futureAllFromPrepareFrame.cancel(true);
        }

        // cancel createBucketEntry
        if (futureCreateBucketEntry != null && !futureCreateBucketEntry.isDone()) {
            BasicUtil.cancelOkHttpCallWithTag(upHttpClient, "createBucketEntry");
            // will cause a CancellationException, and will be caught on this.createBucketEntry
            futureCreateBucketEntry.cancel(true);
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
