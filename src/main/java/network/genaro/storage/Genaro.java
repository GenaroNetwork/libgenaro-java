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
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

import static network.genaro.storage.CryptoUtil.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static network.genaro.storage.Parameters.*;

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
    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private final OkHttpClient okHttpClient = new OkHttpClient.Builder()
            .connectTimeout(GENARO_OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(GENARO_OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(GENARO_OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();

    private String bridgeUrl;
    private GenaroWallet wallet;
    private static final int POINT_PAGE_COUNT = 3;

    public Genaro() {}

    public Genaro(final String bridgeUrl, final GenaroWallet wallet) {
        this.bridgeUrl = bridgeUrl;
        this.wallet = wallet;
    }

    public void Init(final String bridgeUrl, final GenaroWallet wallet) {
        this.bridgeUrl = bridgeUrl;
        this.wallet = wallet;
    }

    public void CheckInit() {
        if (bridgeUrl == null || wallet == null) {
            throw new GenaroRuntimeException("Please pass parameters to Genaro::Genaro or call Genaro::Init first!");
        }
    }

    public String getBridgeUrl() {
        return bridgeUrl;
    }

    public void setBridgeUrl(String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }

    public OkHttpClient getOkHttpClient() {
        return okHttpClient;
    }

    public static String GenaroStrError(int error_code)
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

    public byte[] getPrivateKey() { return wallet.getPrivateKey(); }

    public String getPublicKeyHexString() {
        return wallet.getPublicKeyHexString();
    }

    public String signRequest(final String method, final String path, final String body) throws NoSuchAlgorithmException {
        String msg = method + "\n" + path + "\n" + body;
        return wallet.signMessage(msg);
    }

    public CompletableFuture<String> getInfoFuture() {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            Request request = new Request.Builder()
                    .url(bridgeUrl)
                    .get()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
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
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public String getInfo() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<String> fu = getInfoFuture();
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Bucket> getBucketFuture(final String bucketId) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String signature = signRequest("GET", "/buckets/" + bucketId, "");
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("getBucket")
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 404 || code == 400) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                } else if (code != 200) {
                    throw new GenaroRuntimeException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                Bucket bucket = om.readValue(responseBody, Bucket.class);
                return bucket;
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "getBucket") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public Bucket getBucket(final Uploader uploader, final String bucketId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Bucket> fu = getBucketFuture(bucketId);
        if(uploader != null) {
            uploader.setFutureGetBucket(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Bucket[]> getBucketsFuture() {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String signature = signRequest("GET", "/buckets", "");
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 401) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else if (code != 200 && code != 304) {
                    throw new GenaroRuntimeException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                Bucket[] buckets = om.readValue(responseBody, Bucket[].class);

                // decrypt
                for (Bucket b: buckets) {
                    if (b.getNameIsEncrypted()) {
                        b.setName(CryptoUtil.decryptMetaHmacSha512(b.getName(), wallet.getPrivateKey(), BUCKET_NAME_MAGIC));
                        b.setNameIsEncrypted(false);
                    }
                }

                return buckets;
            } catch (IOException e) {
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public Bucket[] getBuckets() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Bucket[]> fu = getBucketsFuture();
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Boolean> deleteBucketFuture(final String bucketId) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String signature = signRequest("DELETE", "/buckets/" + bucketId, "");
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                ObjectMapper om = new ObjectMapper();
                JsonNode bodyNode = om.readTree(responseBody);

                if (code == 200 || code == 204) {
                    return true;
                } else if (code == 401) {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException("Failed to destroy bucket. (" + code + ")");
                }
            } catch (IOException e) {
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public boolean deleteBucket(final String bucketId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> fu = deleteBucketFuture(bucketId);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Boolean> renameBucketFuture(final String bucketId, final String name) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String encryptedName = CryptoUtil.encryptMetaHmacSha512(BasicUtil.string2Bytes(name), wallet.getPrivateKey(), BUCKET_NAME_MAGIC);
            String jsonStrBody = String.format("{\"name\": \"%s\", \"nameIsEncrypted\": true}", encryptedName);

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature = signRequest("POST", "/buckets/" + bucketId, jsonStrBody);
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                response.close();

                if (code != 200) {
                    throw new GenaroRuntimeException("Request failed with status code: " + code);
                }

                return true;
            } catch (IOException e) {
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public boolean renameBucket(final String bucketId, final String name) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> fu = renameBucketFuture(bucketId, name);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<File> getFileInfoFuture(final String bucketId, final String fileId) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String path = String.format("/buckets/%s/files/%s/info", bucketId, fileId);
            String signature = signRequest("GET", path, "");
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                .tag("getFileInfo")
                .url(bridgeUrl + path)
                .header("x-signature", signature)
                .header("x-pubkey", pubKey)
                .get()
                .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if(code == 403 || code == 401) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else if (code == 404 || code == 400) {
                     throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_FILE_NOTFOUND_ERROR));
                } else if(code == 500) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_INTERNAL_ERROR));
                } else if (code != 200 && code != 304){
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }

                ObjectMapper om = new ObjectMapper();

                File file;
                String realName;
                try {
                    file = om.readValue(responseBody, File.class);
                    realName = CryptoUtil.decryptMetaHmacSha512(file.getFilename(), wallet.getPrivateKey(), Hex.decode(bucketId));
                } catch (Exception e) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_FILEINFO_ERROR));
                }

                file.setFilename(realName);

                String erasureType = file.getErasure().getType();
                if (erasureType != null) {
                    if (erasureType.equals("reedsolomon")) {
                         file.setRs(true);
                    } else {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_FILE_UNSUPPORTED_ERASURE));
                    }
                }

                return file;
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "getFileInfo") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public File getFileInfo(final Downloader downloader, final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<File> fu = getFileInfoFuture(bucketId, fileId);
        if(downloader != null) {
            downloader.setFutureGetFileInfo(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<File[]> listFilesFuture(final String bucketId) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String path = String.format("/buckets/%s/files", bucketId);
            String signature = signRequest("GET", path, "");

            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                ObjectMapper om = new ObjectMapper();

                if (code == 404) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_BUCKET_NOTFOUND_ERROR));
                } else if (code == 400) {
                    throw new GenaroRuntimeException("Bucket id [" + bucketId + "] is invalid");
                } else if (code == 401) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else if (code != 200) {
                    throw new GenaroRuntimeException("Request failed with status code: " + code);
                }

                File[] files = om.readValue(responseBody, File[].class);

                // decrypt
                for (File f : files) {
                    String realName = CryptoUtil.decryptMetaHmacSha512(f.getFilename(), wallet.getPrivateKey(), Hex.decode(bucketId));
                    f.setFilename(realName);
                }

                return files;
            } catch (IOException e) {
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public File[] listFiles(final String bucketId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<File[]> fu = listFilesFuture(bucketId);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    public CompletableFuture<Boolean> deleteFileFuture(final String bucketId, final String fileId) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String path = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String signature = signRequest("DELETE", path, "");
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                ObjectMapper om = new ObjectMapper();
                JsonNode bodyNode = om.readTree(responseBody);

                if (code == 200 || code == 204) {
                    return true;
                } else if (code == 401) {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_AUTH_ERROR));
                } else {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException("Failed to remove file from bucket. (" + code + ")");
                }
            } catch (IOException e) {
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public boolean deleteFile(final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> fu = deleteFileFuture(bucketId, fileId);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<List<Pointer>> getPointersFuture(final String bucketId, final String fileId) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            List<Pointer> ps= new ArrayList<>();

            int skipCount = 0;
            while (true) {
                logger.info("Requesting next set of pointers, total pointers: " + skipCount);
                List<Pointer> psr = this.getPointersRaw(bucketId, fileId, POINT_PAGE_COUNT, skipCount);

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

    public List<Pointer> getPointers(final Downloader downloader, final String bucketId, final String fileId) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<List<Pointer>> fu = getPointersFuture(bucketId, fileId);
        if(downloader != null) {
            downloader.setFutureGetPointers(fu);
        }

        // wait it double seconds
        return fu.get(2 * GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    private CompletableFuture<List<Pointer>> getPointersRawFuture(final String bucketId, final String fileId, final int limit, final int skipCount) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String queryArgs = String.format("limit=%d&skip=%d", limit, skipCount);
            String url = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String path = String.format("%s?%s", url, queryArgs);
            String signature = signRequest("GET", url, queryArgs);
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("getPointersRaw")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                ObjectMapper om = new ObjectMapper();

                logger.info(String.format("Finished request pointers - JSON Response %s", responseBody));

                JsonNode bodyNode = om.readTree(responseBody);

                if (code == 429 || code == 420) {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code != 200) {
                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_POINTER_ERROR));
                }

                List<Pointer> pointers = om.readValue(responseBody, new TypeReference<List<Pointer>>(){});
                pointers.stream().forEach(pointer -> pointer.setMissing(pointer.getToken() == null || pointer.getFarmer() == null));

                return pointers;
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "getPointersRaw") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public List<Pointer> getPointersRaw(final String bucketId, final String fileId, final int limit, final int skipCount) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<List<Pointer>> fu = getPointersRawFuture(bucketId, fileId, limit, skipCount);
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Boolean> isFileExistFuture(final String bucketId, final String encryptedFileName) {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String escapedName = URLEncoder.encode(encryptedFileName, "UTF-8");

            String path = String.format("/buckets/%s/file-ids/%s", bucketId, escapedName);
            String signature = signRequest("GET", path, "");
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("isFileExist")
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                response.close();

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
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public boolean isFileExist(final Uploader uploader, final String bucketId, final String encryptedFileName) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Boolean> fu = isFileExistFuture(bucketId, encryptedFileName);
        if(uploader != null) {
            uploader.setFutureIsFileExists(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }

    CompletableFuture<Frame> requestNewFrameFuture() {
        return BasicUtil.supplyAsync(() -> {

            CheckInit();
            String jsonStrBody = "{}";

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature = signRequest("POST", "/frames", jsonStrBody);
            String pubKey = getPublicKeyHexString();
            Request request = new Request.Builder()
                    .tag("requestNewFrame")
                    .url(bridgeUrl + "/frames")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                int code = response.code();
                String responseBody = response.body().string();

                if (code == 429 || code == 420) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code == 200) {
                    Frame frame = new ObjectMapper().readValue(responseBody, Frame.class);
                    if (frame.getId() != null) {
                        return frame;
                    } else {
                        throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_FRAME_ERROR));
                    }
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_FRAME_ERROR));
                }
            } catch (IOException e) {
                // BasicUtil.cancelOkHttpCallWithTag(okHttpClient, "requestNewFrame") will cause an SocketException
                if (e instanceof SocketException) {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_TRANSFER_CANCELED));
                } else {
                    throw new GenaroRuntimeException(GenaroStrError(GENARO_BRIDGE_REQUEST_ERROR));
                }
            }
        });
    }

    public Frame requestNewFrame(final Uploader uploader) throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Frame> fu = requestNewFrameFuture();
        if(uploader != null) {
            uploader.setFutureRequestNewFrame(fu);
        }
        return fu.get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);
    }
}
