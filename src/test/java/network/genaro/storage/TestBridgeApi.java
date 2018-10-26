package network.genaro.storage;

import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Test()
public class TestBridgeApi {

    public void TestAsync() throws ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        Future<String[]> fu = api.listBuckets();
        String[] bs = fu.get();
        System.out.println(bs);
    }

    public void TestGetInfo() throws ExecutionException, InterruptedException {
        BridgeApi api = new BridgeApi();
        Future<Map<String, Object>> fu = api.getInfo();
        Map<String, Object> info = fu.get();
        System.out.println(info);
    }
}
