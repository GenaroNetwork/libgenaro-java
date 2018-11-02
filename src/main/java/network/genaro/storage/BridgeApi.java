package network.genaro.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import okhttp3.*;
import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static network.genaro.storage.CryptoUtil.*;
import static network.genaro.storage.CryptoUtil.string2Bytes;

public class BridgeApi {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(100, TimeUnit.SECONDS)
            .writeTimeout(100, TimeUnit.SECONDS)
            .readTimeout(300, TimeUnit.SECONDS)
            .build();
    private String bridgeUrl = "http://118.31.61.119:8080"; //http://192.168.0.74:8080
    private GenaroWallet wallet;

    public BridgeApi(String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }

    public BridgeApi() {}

    public void logIn(GenaroWallet wallet) {
        this.wallet = wallet;
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
                        // String bkss = Hex.toHexString(bk);
                        byte[] decryptKey = CryptoUtil.hmacSha512Half(bk, BUCKET_META_MAGIC);
                        // String sss512 = Hex.toHexString(decryptKey2);
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

    public Future<?> downloadFile() {
        return executor.submit(() -> {});
    }

    public Future<?> uploadFile() {
        return executor.submit(() -> {});
    }
}
