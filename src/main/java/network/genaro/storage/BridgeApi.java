package network.genaro.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import okhttp3.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BridgeApi {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private final OkHttpClient client = new OkHttpClient();
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
}
