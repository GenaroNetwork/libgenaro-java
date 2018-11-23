package network.genaro.storage;

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
import java.io.UnsupportedEncodingException;

import java.net.URLEncoder;

import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.*;

import static network.genaro.storage.CryptoUtil.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static network.genaro.storage.Parameters.*;

public class Genaro {
    private static final Logger logger = LogManager.getLogger(Genaro.class);

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(OKHTTP_CONNECT_TIMEOUT, TimeUnit.SECONDS)
            .writeTimeout(OKHTTP_WRITE_TIMEOUT, TimeUnit.SECONDS)
            .readTimeout(OKHTTP_READ_TIMEOUT, TimeUnit.SECONDS)
            .build();
//    private String bridgeUrl = "http://118.31.61.119:8080"; //http://192.168.0.74:8080
//    private String bridgeUrl = "http://192.168.50.206:8080";
    private String bridgeUrl = "http://120.77.247.10:8080";
    private GenaroWallet wallet;
    private static final int POINT_PAGE_COUNT = 3;

    public Genaro(final String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }

    public Genaro() { }

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
            case GENARO_META_ENCRYPTION_ERROR:
                return "Meta encryption error";
            case GENARO_META_DECRYPTION_ERROR:
                return "Meta decryption error";
            case GENARO_TRANSFER_CANCELED:
                return "File transfer canceled";
            case GENARO_MEMORY_ERROR:
                return "Memory error";
            case GENARO_MAPPING_ERROR:
                return "Memory mapped file error";
            case GENARO_UNMAPPING_ERROR:
                return "Memory mapped file unmap error";
            case GENARO_QUEUE_ERROR:
                return "Queue error";
            case GENARO_HEX_DECODE_ERROR:
                return "Unable to decode hex string";
            case GENARO_TRANSFER_OK:
                return "No errors";
            default:
                return "Unknown error";
        }
    }

    public void logIn(final GenaroWallet wallet) {
        this.wallet = wallet;
    }

    public byte[] getPrivateKey() {
        return this.wallet.getPrivateKey();
    }

    private String signRequest(final String method, final String path, final String body) {
        String msg = method + "\n" + path + "\n" + body;
        return wallet.signMessage(msg);
    }

    public Future<Bucket[]> listBuckets() {
//        Preconditions.checkNotNull(this.wallet, "Please login first");
        return executor.submit(() -> {

            String signature = signRequest("GET", "/buckets", "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code == 401) {
                    throw new GenaroException("Invalid user credentials.");
                } else if (code != 200 && code != 304) {
                    throw new GenaroException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                Bucket[] buckets = om.readValue(body, Bucket[].class);

                // decrypt
                for (Bucket b : buckets) {
                    if (b.getNameIsEncrypted()) {
                        byte[] bk = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
                        byte[] decryptKey = CryptoUtil.hmacSha512Half(bk, BUCKET_META_MAGIC);
                        byte[] realName = CryptoUtil.decryptMeta(b.getName(), decryptKey);
                        b.setName(CryptoUtil.bytes2String(realName));
                        b.setNameIsEncrypted(false);
                    }
                }

                return buckets;
            }
        });
    }

    public Future<String> getInfo() {
        return executor.submit(() -> {
            Request request = new Request.Builder()
                    .url(bridgeUrl)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) throw new GenaroException("Unexpected code " + response);

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();

                JsonNode bodyNode = om.readTree(body);
                JsonNode infoNode = bodyNode.get("info");

                String title = infoNode.get("title").asText();
                String description = infoNode.get("description").asText();
                String version = infoNode.get("version").asText();
                String host = bodyNode.get("host").asText();

                return "Title:       " + title + "\n" +
                       "Description: " + description + "\n" +
                       "Version:     " + version + "\n" +
                       "Host:        " + host + "\n";
            }
        });
    }

    /*
    buy bucket through transaction
    public Future<Boolean> addBucket(final String name) {
        return executor.submit(() -> {

            byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
            byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
            byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, string2Bytes(name));
            String encryptedName = encryptMeta(string2Bytes(name), key, nameIv);
            String jsonStrBody = String.format("{\"name\": \"%s\"}", encryptedName);

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);

            String signature = signRequest("POST", "/buckets", jsonStrBody);
            String pubKey = this.wallet.getPublicKeyHexString();

            Request request = new Request.Builder()
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .url(bridgeUrl + "/buckets")
                    //.url("https://postman-echo.com/post")
                    .post(body)
                    .build();

            System.out.println(request.toString());
            try (Response response = client.newCall(request).execute()) {
                String networkResp = response.body().string();
                System.out.println(networkResp);

                return true;
            }

        });
    }
    */
    public Future<Boolean> deleteBucket(final String bucketId) {
        return executor.submit(() -> {

            String signature = signRequest("DELETE", "/buckets/" + bucketId, "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code == 200 || code == 204) {
                    return true;
                } else if (code == 401) {
                    throw new GenaroException("Invalid user credentials.");
                } else {
                    throw new GenaroException("Failed to destroy bucket. (" + code + ")");
                }
            }

        });
    }

    public Future<Boolean> renameBucket(final String bucketId, final String name) {
        return executor.submit(() -> {

            byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
            byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
            byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, string2Bytes(name));
            String encryptedName = encryptMeta(string2Bytes(name), key, nameIv);
            String jsonStrBody = String.format("{\"name\": \"%s\", \"nameIsEncrypted\": true}", encryptedName);

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature = signRequest("POST", "/buckets/" + bucketId, jsonStrBody);
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code != 200) {
                    throw new GenaroException("Request failed with status code: " + code);
                }

                return true;
            }

        });
    }

    public Future<Bucket> getBucket(final String bucketId) {
        return executor.submit(() -> {

            String signature = signRequest("GET", "/buckets/" + bucketId, "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets/" + bucketId)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code != 200) {
                    throw new GenaroException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                Bucket b = om.readValue(body, Bucket.class);
                return b;
            }

        });
    }

    public Future<File[]> listFiles(final String bucketId) {
        return executor.submit(() -> {

            byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(bucketId));
            byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);

            String path = String.format("/buckets/%s/files", bucketId);

            String signature = signRequest("GET", path, "");

            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code == 404) {
                    throw new GenaroException("Bucket id [" + bucketId + "] does not exist");
                } else if (code == 400) {
                    throw new GenaroException("Bucket id [" + bucketId + "] is invalid");
                } else if (code == 401) {
                    throw new GenaroException("Invalid user credentials.");
                } else if (code != 200) {
                    throw new GenaroException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                File[] files = om.readValue(body, File[].class);

                // decrypt
                for (File f : files) {
                    byte[] realName = CryptoUtil.decryptMeta(f.getFilename(), key);
                    f.setFilename(new String(realName));
                }

                return files;
            }
        });
    }

    public Future<Boolean> deleteFile(final String bucketId, final String fileId) {
        return executor.submit(() -> {

            String path = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String signature = signRequest("DELETE", path, "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .delete()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code == 200 || code == 204) {
                    return true;
                } else if (code == 401) {
                    throw new GenaroException("Invalid user credentials.");
                } else {
                    throw new GenaroException("Failed to remove file from bucket. (" + code + ")");
                }
            }

        });
    }

    Future<File> getFileInfo(final String bucketId, final String fileId) {
        return executor.submit(() -> {

            byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(bucketId));
            byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);

            String path = String.format("/buckets/%s/files/%s/info", bucketId, fileId);
            String signature = signRequest("GET", path, "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                if (code == 404) {
                    throw new GenaroException("Bucket id [" + fileId + "] does not exist");
                } else if (code != 200){
                    throw new GenaroException("Request failed with status code: " + code);
                }

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                File f = om.readValue(body, File.class);
                byte[] realName = CryptoUtil.decryptMeta(f.getFilename(), key);
                f.setFilename(new String(realName));
                return f;
            }

        });
    }

    Future<List<Pointer>> getPointers(final String bucketId, final String fileId) {
        return executor.submit(() -> {

            List<Pointer> ps= new ArrayList<>();

            int skipCount = 0;
            while (true) {
                logger.info("Requesting next set of pointers, total pointers: " + skipCount);
                List<Pointer> psr = this.getPointersRaw(bucketId, fileId, POINT_PAGE_COUNT, skipCount).get(GENARO_HTTP_TIMEOUT, TimeUnit.SECONDS);

                if(psr.size() == 0) break;
                skipCount += psr.size();
                ps.addAll(psr);
            }

            return ps;
        });
    }

    private Future<List<Pointer>> getPointersRaw(final String bucketId, final String fileId, int limit, int skipCount) {
        return executor.submit(() -> {

            String queryArgs = String.format("limit=%d&skip=%d", limit, skipCount);
            String url = String.format("/buckets/%s/files/%s", bucketId, fileId);
            String path = String.format("%s?%s", url, queryArgs);
            String signature = signRequest("GET", url, queryArgs);
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                int code = response.code();

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();

                if (code == 429 || code == 420) {
                    throw new GenaroException(Genaro.GenaroStrError(GENARO_BRIDGE_RATE_ERROR));
                } else if (code != 200){
                    JsonNode bodyNode = om.readTree(body);

                    logger.error(bodyNode.get("error").asText());
                    throw new GenaroException(Genaro.GenaroStrError(GENARO_BRIDGE_POINTER_ERROR));
                }

                List<Pointer> pointers = om.readValue(body, new TypeReference<List<Pointer>>(){});
                return pointers;
            }

        });
    }

    Future<Boolean> isFileExist(final String bucketId, final String encryptedFileName) throws UnsupportedEncodingException {
        byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(bucketId));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        String escapedName = URLEncoder.encode(encryptedFileName, "UTF-8");

        return executor.submit(() -> {

            String path = String.format("/buckets/%s/file-ids/%s", bucketId, escapedName);
            String signature = signRequest("GET", path, "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .get()
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.code() == 404) {
                    return false;
                } else if (response.code() == 200) {
                    return true;
                } else {
                    throw new Exception("Request file-ids failed");
                }
            }

        });
    }

    Future<Frame> requestNewFrame() {

        String jsonStrBody = "{}";

        return executor.submit(() -> {

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(JSON, jsonStrBody);
            String signature = signRequest("POST", "/frames", jsonStrBody);
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/frames")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .post(body)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.code() != 200) {
                    throw new GenaroException("Request frame id error");
                } else {
                    ObjectMapper om = new ObjectMapper();
                    String responseBody = response.body().string();
                    return om.readValue(responseBody, Frame.class);
                }
            }

        });
    }
}
