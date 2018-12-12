package network.genaro.storage;

interface DownloadCallback {
    default void onBegin() {
//        System.out.println("Download started");
    }

    default void onFinish() {
//        System.out.println("Download finished");
    }

    default void onFail(String error) {
//        System.out.println("Download failed, reason: " + error != null ? error : "Unknown");
    }

    // after Uploader::cancel is called
    default void onCancel() { }

    /**
     * called when progress update
     * @param progress range from 0 to 1
     */
    default void onProgress(float progress) { }
}

interface UploadCallback {
    default void onBegin(long fileSize) {
//        System.out.println("Upload started");
    }

    default void onFinish(String fileId) {
//        System.out.println("Upload finished, fileId: " + fileId);
    }

    default void onFail(String error) {
//        System.out.println("Upload failed, reason: " + error != null ? error : "Unknown");
    }

    // after Uploader::cancel is called
    default void onCancel() { }

    /**
     * called when progress update
     * @param progress range from 0 to 1
     */
    default void onProgress(float progress) { }
}
