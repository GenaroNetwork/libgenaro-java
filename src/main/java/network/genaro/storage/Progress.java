package network.genaro.storage;

public interface Progress {

    void onBegin();
    void onEnd(int status);

    /**
     * called when progress update
     * @param progress range from 0 to 1
     */
    void onProgress(float progress, String message);
}
