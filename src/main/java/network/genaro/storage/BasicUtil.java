package network.genaro.storage;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class BasicUtil {
    public static byte[] string2Bytes(final String input) {
        return input.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytes2String(final byte[] input) {
        return new String(input, StandardCharsets.UTF_8);
    }
    // This is basically doing the same as the CompletableFuture::supplyAsync(Supplier<U> supplier), but allowing checked exceptions
    public static <T> CompletableFuture<T> supplyAsync(Callable<T> c) {
        CompletableFuture<T> f = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try { f.complete(c.call()); } catch(Throwable t) { f.completeExceptionally(t); }
        });
        return f;
    }

    // This is basically doing the same as the CompletableFuture::supplyAsync(Supplier<U> supplier, Executor executor), but allowing checked exceptions
    public static <T> CompletableFuture<T> supplyAsync(Callable<T> c, Executor executor) {
        CompletableFuture<T> f = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try { f.complete(c.call()); } catch(Throwable t) { f.completeExceptionally(t); }
        }, executor);
        return f;
    }
}
