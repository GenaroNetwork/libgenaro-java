package network.genaro.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import okhttp3.*;
import org.bouncycastle.util.encoders.Hex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static network.genaro.storage.CryptoUtil.BUCKET_META_MAGIC;
import static network.genaro.storage.CryptoUtil.BUCKET_NAME_MAGIC;

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
                        byte[] decryptKey = CryptoUtil.hmacSha512(bk, BUCKET_META_MAGIC);
                        byte[] decryptKey2 = Arrays.copyOfRange(decryptKey, 0, 32);
                        // String sss512 = Hex.toHexString(decryptKey2);
                        byte[] realnameba = CryptoUtil.decryptMeta(b.getName(), decryptKey2);
                        b.setName(new String(realnameba));
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

    public Future<?> addBucket() {
        return executor.submit(() -> {});
    }

    public Future<?> removeBucket() {
        return executor.submit(() -> {});
    }

    public Future<?> renameBucket() {
        return executor.submit(() -> {});
    }

    public Future<File[]> listFiles() {
        return executor.submit(() -> new File[0]);
    }

    public Future<?> removeFile() {
        return executor.submit(() -> {});
    }

    public Future<?> downloadFile() {
        return executor.submit(() -> {});
    }

    public Future<?> uploadFile() {
        return executor.submit(() -> {});
    }
}
