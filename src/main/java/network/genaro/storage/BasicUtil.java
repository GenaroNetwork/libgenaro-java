package network.genaro.storage;

import okhttp3.Call;
import okhttp3.OkHttpClient;

import java.nio.charset.StandardCharsets;
//import java.util.concurrent.Callable;
//import java.util.concurrent.CompletableFuture;

final class BasicUtil {
    static byte[] string2Bytes(final String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }

    static String bytes2String(final byte[] input) {
        return new String(input, StandardCharsets.UTF_8);
    }

//    // This is basically doing the same as the CompletableFuture::supplyAsync(Supplier<U> supplier), but allowing checked exceptions
//    public static <T> CompletableFuture<T> supplyAsync(Callable<T> c) {
//        CompletableFuture<T> f = new CompletableFuture<>();
//        CompletableFuture.runAsync(() -> {
//            try { f.complete(c.call()); } catch(Throwable t) { f.completeExceptionally(t); }
//        });
//        return f;
//    }

//    // This is basically doing the same as the CompletableFuture::supplyAsync(Supplier<U> supplier, Executor executor), but allowing checked exceptions
//    public static <T> CompletableFuture<T> supplyAsync(Callable<T> c, Executor executor) {
//        CompletableFuture<T> f = new CompletableFuture<>();
//        CompletableFuture.runAsync(() -> {
//            try { f.complete(c.call()); } catch(Throwable t) { f.completeExceptionally(t); }
//        }, executor);
//        return f;
//    }

    static void cancelOkHttpCallWithTag(OkHttpClient client, Object tag) {
        for(Call call: client.dispatcher().queuedCalls()) {
            if(call.request().tag().equals(tag)) {
                call.cancel();
            }
        }

        for(Call call: client.dispatcher().runningCalls()) {
            if(call.request().tag().equals(tag)) {
                call.cancel();
            }
        }
    }
}
