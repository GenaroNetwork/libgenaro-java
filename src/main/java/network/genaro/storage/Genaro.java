package network.genaro.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.MediaType;
import okhttp3.RequestBody;

import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.web3j.crypto.CipherException;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.Parameters.*;
import static network.genaro.storage.Pointer.PointerStatus.*;
import network.genaro.storage.GenaroCallback.*;

class ShardMeta {
    private String hash;
    private byte[][] challenges;    // [GENARO_SHARD_CHALLENGES][32]
    private String[] challengesAsStr;  // [GENARO_SHARD_CHALLENGES]

    // Merkle Tree leaves. Each leaf is size of RIPEMD160 hash
    private String[] tree;  // [GENARO_SHARD_CHALLENGES]
    private int index;
    private boolean isParity;
    private long size;

    public ShardMeta(int index) { this.setIndex(index); }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public byte[][] getChallenges() { return challenges; }

    public void setChallenges(byte[][] challenges) {
        this.challenges = challenges;
    }

    public String[] getChallengesAsStr() {
        return challengesAsStr;
    }

    public void setChallengesAsStr(String[] challengesAsStr) {
        this.challengesAsStr = challengesAsStr;
    }

    public String[] getTree() {
        return tree;
    }

    public void setTree(String[] tree) {
        this.tree = tree;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean getParity() {
        return isParity;
    }

    public void setParity(boolean parity) {
        isParity = parity;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
class FarmerPointer {
    private String token;
    private Farmer farmer;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Farmer getFarmer() {
        return farmer;
    }

    public void setFarmer(Farmer farmer) {
        this.farmer = farmer;
    }
}

class ShardTracker {
    //    int progress;
    private int index;
    private FarmerPointer pointer;
    private ShardMeta meta;
    private long uploadedSize;
    private String shardFile;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public FarmerPointer getPointer() {
        return pointer;
    }

    public void setPointer(FarmerPointer pointer) {
        this.pointer = pointer;
    }

    public ShardMeta getMeta() {
        return meta;
    }

    public void setMeta(ShardMeta meta) {
        this.meta = meta;
    }

    public long getUploadedSize() {
        return uploadedSize;
    }

    public void setUploadedSize(long uploadedSize) {
        this.uploadedSize = uploadedSize;
    }

    public String getShardFile() { return shardFile; }

    public void setShardFile(String shardFile) { this.shardFile = shardFile; }
}

public class Genaro {
    private final Logger logger = LogManager.getLogger(Genaro.class);
    private static final int POINT_PAGE_COUNT = 3;

    private String bridgeUrl;
    private GenaroWallet wallet;

    private OkHttpClient genaroHttpClient = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    public Genaro() {}

    public Genaro(final String bridgeUrl) {
        init(bridgeUrl);
    }

    public Genaro(final String bridgeUrl, final String privKey, final String passwd) throws CipherException, IOException {
        init(bridgeUrl, privKey, passwd);
    }

    public void init(final String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }

    public void init(final String bridgeUrl, final String privKey, final String passwd) throws CipherException, IOException {
        this.bridgeUrl = bridgeUrl;
        GenaroWallet wallet = new GenaroWallet(privKey, passwd);
        this.wallet = wallet;
    }

    private void checkInit(boolean checkWallet) {
        if (bridgeUrl == null || (checkWallet && wallet == null)) {
            throw new GenaroRuntimeException("Bridge url or wallet has not been initialized!");
        }
    }

    public String getBridgeUrl() {
        return bridgeUrl;
    }

    public void setBridgeUrl(String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }

    public static String genaroStrError(int error_code)
    {
        switch(error_code) {
            case GENARO_BRIDGE_REQUEST_ERROR:
                return "Bridge request error";
            case GENARO_BRIDGE_AUTH_ERROR:
                return "Bridge request authorization error";
            case GENARO_BRIDGE_TOKEN_ERROR:
                return "Bridge request token error";
            case GENARO_BRIDGE_POINTER_ERROR:
                return "Bridge request pointer error";
            case GENARO_BRIDGE_REPOINTER_ERROR:
                return "Bridge request replace pointer error";
            case GENARO_BRIDGE_TIMEOUT_ERROR:
                return "Bridge request timeout error";
            case GENARO_BRIDGE_INTERNAL_ERROR:
                return "Bridge request internal error";
            case GENARO_BRIDGE_RATE_ERROR:
                return "Bridge rate limit error";
            case GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR:
                return "Bucket is not found";
            case GENARO_BRIDGE_FILE_NOTFOUND_ERROR:
                return "File is not found";
            case GENARO_BRIDGE_BUCKET_FILE_EXISTS:
                return "File already exists";
            case GENARO_BRIDGE_OFFER_ERROR:
                return "Unable to receive storage offer";
            case GENARO_BRIDGE_JSON_ERROR:
                return "Unexpected JSON response";
            case GENARO_BRIDGE_FILEINFO_ERROR:
                return "Bridge file info error";
            case GENARO_BRIDGE_DECRYPTION_KEY_ERROR:
                return "Bridge request decryption key error";
            case GENARO_FARMER_REQUEST_ERROR:
                return "Farmer request error";
            case GENARO_FARMER_EXHAUSTED_ERROR:
                return "Farmer exhausted error";
            case GENARO_FARMER_TIMEOUT_ERROR:
                return "Farmer request timeout error";
            case GENARO_FARMER_AUTH_ERROR:
                return "Farmer request authorization error";
            case GENARO_FARMER_INTEGRITY_ERROR:
                return "Farmer request integrity error";
            case GENARO_FILE_INTEGRITY_ERROR:
                return "File integrity error";
            case GENARO_FILE_READ_ERROR:
                return "File read error";
            case GENARO_FILE_WRITE_ERROR:
                return "File write error";
            case GENARO_BRIDGE_FRAME_ERROR:
                return "Bridge frame request error";
            case GENARO_FILE_ENCRYPTION_ERROR:
                return "File encryption error";
            case GENARO_FILE_SIZE_ERROR:
                return "File size error";
            case GENARO_FILE_DECRYPTION_ERROR:
                return "File decryption error";
            case GENARO_FILE_GENERATE_HMAC_ERROR:
                return "File hmac generation error";
            case GENARO_FILE_SHARD_MISSING_ERROR:
                return "File missing shard error";
            case GENARO_FILE_RECOVER_ERROR:
                return "File recover error";
            case GENARO_FILE_RESIZE_ERROR:
                return "File resize error";
            case GENARO_FILE_UNSUPPORTED_ERASURE:
                return "File unsupported erasure code error";
            case GENARO_FILE_PARITY_ERROR:
                return "File create parity error";
            case GENARO_TRANSFER_CANCELED:
                return "File transfer canceled";
            case GENARO_TRANSFER_OK:
                return "No errors";
            case GENARO_ALGORITHM_ERROR:
                return "Algorithm error";
            default:
                return "Unknown error";
        }
    }

    byte[] getPrivateKey() { return wallet.getPrivateKey(); }

    String getPublicKeyHexString() {
        return wallet.getPublicKeyHexString();
    }

    String signRequest(final String method, final String path, final String body) throws NoSuchAlgorithmException {
        String msg = method + "\n" + path + "\n" + body;
        return wallet.signMessage(msg);
    }

    public CompletableFuture<String> getInfoFuture() {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(false);
            Request request = new Request.Builder()
                    .url(bridgeUrl)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                String responseBody = response.body().string();

                if (!response.isSuccessful()) throw new GenaroRuntimeException("Unexpected code " + response);

                ObjectMapper om = new ObjectMapper();

                JsonNode bodyNode = om.readTree(responseBody);
                JsonNode infoNode = bodyNode.get("info");

                String title = infoNode.get("title").asText();
                String description = infoNode.get("description").asText();
                String version = infoNode.get("version").asText();
                String host = bodyNode.get("host").asText();

                return "Title:       " + title + "\n" +
                       "Description: " + description + "\n" +
                       "Version:     " + version + "\n" +
                       "Host:        " + host + "\n";
            } catch (IOException e) {
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public String getInfo() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<String> fu = getInfoFuture();
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Bucket> getBucketFuture(final Uploader uploader, final String bucketId) {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(true);
            String signature;
            try {
                signature = signRequest("GET", "/buckets/" + bucketId, "");
            } catch (NoSuchAlgorithmException e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("getBucket")
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(uploader != null) {
                okHttpClient = uploader.getUpHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 404 || code == 400) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                } else if (code != 200) {
                    throw new GenaroRuntimeException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                Bucket bucket = om.readValue(responseBody, Bucket.class);
                return bucket;
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "getBucket") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    Bucket getBucket(final Uploader uploader, final String bucketId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Bucket> fu = getBucketFuture(uploader, bucketId);
        if(uploader != null) {
            uploader.setFutureGetBucket(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Void> getBuckets(final GetBucketsCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            checkInit(true);
            String signature;
            try {
                signature = signRequest("GET", "/buckets", "");
            } catch (NoSuchAlgorithmException e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code != 200 && code != 304) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                ObjectMapper om = new ObjectMapper();
                Bucket[] buckets = om.readValue(responseBody, Bucket[].class);

                try {
                    // decrypt
                    for (Bucket b : buckets) {
                        if (b.getNameIsEncrypted()) {
                            b.setName(CryptoUtil.decryptMetaHmacSha512(b.getName(), getPrivateKey(), BUCKET_NAME_MAGIC));
                            b.setNameIsEncrypted(false);
                        }
                    }
                } catch (Exception e) {
                    callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                    return null;
                }

                callback.onFinish(buckets);
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    public CompletableFuture<Void> deleteBucket(final String bucketId, final DeleteBucketCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            checkInit(true);
            String signature;
            try {
                signature = signRequest("DELETE", "/buckets/" + bucketId, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                    return null;
                } else if (code != 200 && code != 204) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                callback.onFinish();
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    public CompletableFuture<Void> renameBucket(final String bucketId, final String name, final RenameBucketCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            checkInit(true);
            String encryptedName;
            try {
                encryptedName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(name), getPrivateKey(), BUCKET_NAME_MAGIC);
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String jsonStrBody = String.format("{\"name\": \"%s\", \"nameIsEncrypted\": true}", encryptedName);

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature;
            try {
                signature = signRequest("POST", "/buckets/" + bucketId, jsonStrBody);
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                    return null;
                } else if (code != 200) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                callback.onFinish();
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    CompletableFuture<File> getFileInfoFuture(final Downloader downloader, final String bucketId, final String fileId) {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(true);
            String path = String.format("/buckets/%s/files/%s/info", bucketId, fileId);
            String signature;
            try {
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                .tag("getFileInfo")
                .url(bridgeUrl + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .get()
                .build();

            OkHttpClient okHttpClient;
            if(downloader != null) {
                okHttpClient = downloader.getDownHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if(code == 403 || code == 401) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else if (code == 404 || code == 400) {
                     throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FILE_NOTFOUND_ERROR));
                } else if(code == 500) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_INTERNAL_ERROR));
                } else if (code != 200 && code != 304){
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }

                ObjectMapper om = new ObjectMapper();

                File file;
                String realName;
                try {
                    file = om.readValue(responseBody, File.class);
                    realName = CryptoUtil.decryptMetaHmacSha512(file.getFilename(), getPrivateKey(), Hex.decode(bucketId));
                } catch (Exception e) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FILEINFO_ERROR));
                }

                file.setFilename(realName);

                String erasureType = file.getErasure().getType();
                if (erasureType != null) {
                    if (erasureType.equals("reedsolomon")) {
                         file.setRs(true);
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_FILE_UNSUPPORTED_ERASURE));
                    }
                }

                return file;
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "getFileInfo") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    File getFileInfo(final Downloader downloader, final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<File> fu = getFileInfoFuture(downloader, bucketId, fileId);
        if(downloader != null) {
            downloader.setFutureGetFileInfo(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Void> listFiles(final String bucketId, final ListFilesCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            checkInit(true);
            String path = String.format("/buckets/%s/files", bucketId);
            String signature;
            try {
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }

            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                ObjectMapper om = new ObjectMapper();

                if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                    return null;
                } else if (code == 400) {
                    callback.onFail("Bucket id [" + bucketId + "] is invalid");
                    return null;
                } else if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code != 200) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                File[] files = om.readValue(responseBody, File[].class);

                try {
                    // decrypt
                    for (File f : files) {
                        String realName = CryptoUtil.decryptMetaHmacSha512(f.getFilename(), getPrivateKey(), Hex.decode(bucketId));
                        f.setFilename(realName);
                    }
                } catch (Exception e) {
                    callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                    return null;
                }

                callback.onFinish(files);
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    public CompletableFuture<Void> deleteFile(final String bucketId, final String fileId, final DeleteFileCallback callback) {
        return CompletableFuture.supplyAsync(() -> {
            checkInit(true);
            String path = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String signature;
            try {
                signature = signRequest("DELETE", path, "");
            } catch (Exception e) {
                callback.onFail(genaroStrError(GENARO_ALGORITHM_ERROR));
                return null;
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = genaroHttpClient.newCall(request).execute()) {
                int code = response.code();

                if (code == 401) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                    return null;
                } else if (code == 404) {
                    callback.onFail(genaroStrError(GENARO_BRIDGE_FILE_NOTFOUND_ERROR));
                    return null;
                } else if (code != 200 && code != 204){
                    callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                    return null;
                }

                callback.onFinish();
            } catch (SocketTimeoutException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_TIMEOUT_ERROR));
                return null;
            } catch (IOException e) {
                callback.onFail(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                return null;
            }

            return null;
        });
    }

    CompletableFuture<List<Pointer>> getPointersFuture(final Downloader downloader, final String bucketId, final String fileId) {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(true);
            List<Pointer> ps= new ArrayList<>();

            int skipCount = 0;
            while (true) {
                logger.info("Requesting next set of pointers, total pointers: " + skipCount);

                List<Pointer> psr;
                try {
                    psr = this.getPointersRaw(downloader, bucketId, fileId, POINT_PAGE_COUNT, skipCount);
                } catch (Exception e) {
                    // TODO: it's not properly
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }

                if(psr.size() == 0) {
                    logger.info("Finished requesting pointers");
                    break;
                }

                skipCount += psr.size();
                ps.addAll(psr);
            }

            return ps;
        });
    }

    List<Pointer> getPointers(final Downloader downloader, final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<List<Pointer>> fu = getPointersFuture(downloader, bucketId, fileId);
        if(downloader != null) {
            downloader.setFutureGetPointers(fu);
        }

        // wait it double seconds
        return fu.get(2 * GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    private CompletableFuture<List<Pointer>> getPointersRawFuture(final Downloader downloader, final String bucketId, final String fileId, final int limit, final int skipCount) {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(true);
            String queryArgs = String.format("limit=%d&skip=%d", limit, skipCount);
            String url = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String path = String.format("%s?%s", url, queryArgs);
            String signature;
            try {
                signature = signRequest("GET", url, queryArgs);
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("getPointersRaw")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(downloader != null) {
                okHttpClient = downloader.getDownHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                ObjectMapper om = new ObjectMapper();

                logger.info(String.format("Finished request pointers - JSON Response %s", responseBody));

                JsonNode bodyNode = om.readTree(responseBody);

                if (code == 429 || code == 420) {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code != 200) {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_POINTER_ERROR));
                }

                List<Pointer> pointers = om.readValue(responseBody, new TypeReference<List<Pointer>>(){});
                pointers.stream().forEach(pointer -> {
                    if (pointer.getToken() == null || pointer.getFarmer() == null) {
                        pointer.setStatus(POINTER_MISSING);
                    }
                });

                return pointers;
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "getPointersRaw") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    private List<Pointer> getPointersRaw(final Downloader downloader, final String bucketId, final String fileId, final int limit, final int skipCount) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<List<Pointer>> fu = getPointersRawFuture(downloader, bucketId, fileId, limit, skipCount);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Boolean> isFileExistFuture(final Uploader uploader, final String bucketId, final String encryptedFileName) {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(true);
            String escapedName;
            String path;
            String signature;
            try {
                escapedName = URLEncoder.encode(encryptedFileName, "UTF-8");
                path = String.format("/buckets/%s/file-ids/%s", bucketId, escapedName);
                signature = signRequest("GET", path, "");
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }

            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("isFileExist")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            OkHttpClient okHttpClient;
            if(uploader != null) {
                okHttpClient = uploader.getUpHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();

                if (code == 404) {
                    return false;
                } else if (code == 200) {
                    return true;
                } else {
                    throw new GenaroRuntimeException("Request file-ids failed");
                }
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "isFileExist") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    boolean isFileExist(final Uploader uploader, final String bucketId, final String encryptedFileName) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> fu = isFileExistFuture(uploader, bucketId, encryptedFileName);
        if(uploader != null) {
            uploader.setFutureIsFileExists(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Frame> requestNewFrameFuture(final Uploader uploader) {
        return CompletableFuture.supplyAsync(() -> {

            checkInit(true);
            String jsonStrBody = "{}";

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature;
            try {
                signature = signRequest("POST", "/frames", jsonStrBody);
            } catch (Exception e) {
                throw new GenaroRuntimeException(genaroStrError(GENARO_ALGORITHM_ERROR));
            }
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("requestNewFrame")
                    .url(bridgeUrl + "/frames")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            OkHttpClient okHttpClient;
            if(uploader != null) {
                okHttpClient = uploader.getUpHttpClient();
            } else {
                okHttpClient = genaroHttpClient;
            }

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 429 || code == 420) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code == 200) {
                    Frame frame = new ObjectMapper().readValue(responseBody, Frame.class);
                    if (frame.getId() != null) {
                        return frame;
                    } else {
                        throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FRAME_ERROR));
                    }
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_FRAME_ERROR));
                }
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "requestNewFrame") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(genaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    Frame requestNewFrame(final Uploader uploader) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Frame> fu = requestNewFrameFuture(uploader);
        if(uploader != null) {
            uploader.setFutureRequestNewFrame(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public Downloader resolveFile(final String bucketId, final String fileId, final String path, final boolean overwrite, final ResolveFileCallback callback) {
        Downloader downloader = new Downloader(this, bucketId, fileId, path, overwrite, callback);
        CompletableFuture<Void> fu = CompletableFuture.runAsync(downloader);
        downloader.setFutureBelongsTo(fu);

        return downloader;
    }

    public Uploader storeFile(final boolean rs, final String filePath, final String fileName, final String bucketId, final StoreFileCallback callback) {
        Uploader uploader = new Uploader(this, rs, filePath, fileName, bucketId, callback);
        CompletableFuture<Void> fu = CompletableFuture.runAsync(uploader);
        uploader.setFutureBelongsTo(fu);

        return uploader;
    }
}
