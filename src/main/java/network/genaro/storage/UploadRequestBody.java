package network.genaro.storage;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

final class UploadRequestBody extends RequestBody {
    public interface ProgressListener {
        void transferred(long delta);
    }

    private static final int SEGMENT_SIZE = 2 * 1024;

    private FileChannel inputChannel;
    private long position;
    private long size;

    private ProgressListener listener;
    private String contentType;

    public UploadRequestBody(FileChannel inputChannel, long position, long size, String contentType, ProgressListener listener) {
        this.inputChannel = inputChannel;
        this.position = position;
        this.size = size;
        this.contentType = contentType;
        this.listener = listener;
    }

    @Override
    public long contentLength() {
        return size;
    }

    @Override
    public MediaType contentType() {
        return MediaType.parse(contentType);
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        ByteBuffer dataBuffer = ByteBuffer.allocate(SEGMENT_SIZE);
        byte[] mBlock = new byte[SEGMENT_SIZE];
        int delta;
        long readBytes = 0;

        while ((delta = inputChannel.read(dataBuffer, position)) != -1 && readBytes < size) {
            dataBuffer.flip();
            dataBuffer.get(mBlock, 0, delta);
            dataBuffer.flip();

            sink.write(mBlock, 0, delta);
            position += delta;
            readBytes += delta;

            if (listener != null) {
                listener.transferred(delta);
            }
        }
    }
}
