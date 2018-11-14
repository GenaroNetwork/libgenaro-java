package network.genaro.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import okhttp3.*;
import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static network.genaro.storage.CryptoUtil.*;

public class BridgeApi {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(100, TimeUnit.SECONDS)
            .writeTimeout(100, TimeUnit.SECONDS)
            .readTimeout(300, TimeUnit.SECONDS)
            .build();
    private String bridgeUrl = "http://118.31.61.119:8080"; //http://192.168.0.74:8080
    private GenaroWallet wallet;
    private static final int POINT_PAGE_COUNT = 5;

    public BridgeApi(String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }

    public BridgeApi() {}

    public void logIn(GenaroWallet wallet) {
        this.wallet = wallet;
    }

    public byte[] getPrivateKey() {
        return this.wallet.getPrivateKey();
    }

    private String signRequest(String method, String path, String body) {
        String msg = method + "\n" + path + "\n" + body;
        return wallet.signMessage(msg);
    }

    public Future<Bucket[]> listBuckets() {
        Preconditions.checkNotNull(this.wallet, "Please login first");
        return executor.submit(() -> {

            String signature = signRequest("GET", "/buckets", "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + "/buckets")
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                Bucket[] buckets = om.readValue(body, Bucket[].class);

                // decrypt
                for (Bucket b : buckets) {
                    if (b.isNameIsEncrypted()) {
                        byte[] bk = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
                        byte[] decryptKey = CryptoUtil.hmacSha512Half(bk, BUCKET_META_MAGIC);
                        byte[] realName = CryptoUtil.decryptMeta(b.getName(), decryptKey);
                        b.setName(new String(realName));
                        b.setNameIsEncrypted(false);
                    }
                }

                return buckets;
            }
        });
    }

    public Future<Map<String, Object>> getInfo() {
        return executor.submit(() -> {
            Request request = new Request.Builder()
                    .url(bridgeUrl)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();

                JsonNode jsonNode = om.readTree(body);
                JsonNode info = jsonNode.get("info");

                return om.convertValue(info, new TypeReference<Map<String,Object>>(){});
            }
        });
    }

    /*
    buy bucket through transaction
    public Future<?> addBucket(String name) {
        byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, string2Bytes(name));
        String encryptedName = encryptMeta(string2Bytes(name), key, nameIv);
        String jsonStrBody = String.format("{\"name\": \"%s\"}", encryptedName);

        return executor.submit(() -> {
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
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
    }
    */
    public Future<?> deleteBucket(final String bucketId) {
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
                if (response.code() == 404) {
                    throw new IOException("bucket not found, id: " + bucketId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
    }

    public Future<?> renameBucket(final String bucketId, final String name) {

        byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(BUCKET_NAME_MAGIC));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        byte[] nameIv = CryptoUtil.hmacSha512Half(bucketKey, string2Bytes(name));
        String encryptedName = encryptMeta(string2Bytes(name), key, nameIv);
        String jsonStrBody = String.format("{\"name\": \"%s\", \"nameIsEncrypted\": true}", encryptedName);

        return executor.submit(() -> {

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
                if (response.code() != 200) {
                    throw new IOException("bucket not found, id: " + bucketId);
                }
            } catch (IOException e) {
                e.printStackTrace();
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
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.code() != 200) {
                    throw new IOException("bucket not found, id: " + bucketId);
                }

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                Bucket b = om.readValue(body, Bucket.class);
                return b;
            }

        });
    }

    public Future<File[]> listFiles(final String bucketId) {
        byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(bucketId));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        return executor.submit(() -> {

            String path = String.format("/buckets/%s/files", bucketId);

            String signature = signRequest("GET", path, "");

            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);
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

    public Future<?> deleteFile(final String bucketId, final String fileId) {
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
                if (response.code() != 204) {
                    throw new IOException("file not found, id: " + bucketId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
    }

    public Future<File> getFileInfo(final String bucketId, final String fileId) {
        byte[] bucketKey = CryptoUtil.generateBucketKey(wallet.getPrivateKey(), Hex.decode(bucketId));
        byte[] key = CryptoUtil.hmacSha512Half(bucketKey, BUCKET_META_MAGIC);
        return executor.submit(() -> {

            String path = String.format("/buckets/%s/files/%s/info", bucketId, fileId);
            String signature = signRequest("GET", path, "");
            String pubKey = this.wallet.getPublicKeyHexString();
            Request request = new Request.Builder()
                    .url(bridgeUrl + path)
                    .header("x-signature", signature)
                    .header("x-pubkey", pubKey)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.code() != 200) {
                    throw new IOException("file not found, id: " + fileId);
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


    public Future<List<Pointer>> getPointers(final String bucketId, final String fileId) throws ExecutionException, InterruptedException {

        return executor.submit(() -> {
            List<Pointer> ps= new ArrayList<>();

            int skipCount = 0;
            while (true) {
                List<Pointer> psr = this.getPointersRaw(bucketId, fileId, POINT_PAGE_COUNT, skipCount).get();
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
                    .build();

            try (Response response = client.newCall(request).execute()) {
                if (response.code() != 200) {
                    throw new IOException("file not found, id: " + fileId);
                }

                ObjectMapper om = new ObjectMapper();
                String body = response.body().string();
                List<Pointer> pointers = om.readValue(body, new TypeReference<List<Pointer>>(){});
                return pointers;
            }

        });
    }


    public Future<Boolean> isFileExist(final String bucketId, final String encryptedFileName) throws UnsupportedEncodingException {
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

    public Future<Frame> requestNewFrame() {

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
                    throw new IOException("Request frame id error");
                } else {
                    ObjectMapper om = new ObjectMapper();
                    String responseBody = response.body().string();
                    return om.readValue(responseBody, Frame.class);
                }
            }

        });
    }
}
