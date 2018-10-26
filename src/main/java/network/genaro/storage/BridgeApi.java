package network.genaro.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BridgeApi {

    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private final OkHttpClient client = new OkHttpClient();
    private String bridgeUrl = "http://118.31.61.119:8080"; //http://192.168.0.74:8080

    public BridgeApi(String bridgeUrl) {
        this.bridgeUrl = bridgeUrl;
    }
    public BridgeApi() {
    }

    public Future<String[]> listBuckets() {
        return executor.submit(() -> new String[]{});
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
