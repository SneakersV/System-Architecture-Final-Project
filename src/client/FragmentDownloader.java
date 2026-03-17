package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimized worker thread that maintains a persistent TCP connection to a Daemon.
 * It reuses the same socket for all chunks requested from that source.
 */
public class FragmentDownloader extends Thread {

    private String targetIp;
    private int targetPort;
    private String filename;
    private FileChannel fileChannel;
    private int sourceIndex;
    
    private ConcurrentLinkedQueue<Integer> chunkQueue;
    private int chunkSize;
    private long fileSize;
    private AtomicLong globalDownloadedCounter;
    
    private boolean running = true;
    private long bytesDownloadedInSession = 0;

    public FragmentDownloader(String targetIp, int targetPort, String filename, FileChannel fileChannel, 
                             int sourceIndex, ConcurrentLinkedQueue<Integer> chunkQueue, 
                             int chunkSize, long fileSize, AtomicLong globalDownloadedCounter) {
        this.targetIp = targetIp;
        this.targetPort = targetPort;
        this.filename = filename;
        this.fileChannel = fileChannel;
        this.sourceIndex = sourceIndex;
        this.chunkQueue = chunkQueue;
        this.chunkSize = chunkSize;
        this.fileSize = fileSize;
        this.globalDownloadedCounter = globalDownloadedCounter;
    }

    @Override
    public void run() {
        Integer currentChunk = null;
        try (Socket socket = new Socket(targetIp, targetPort)) {
            socket.setSoTimeout(10000); // 10s timeout
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            byte[] buffer = new byte[65536]; // 64KB buffer for high-speed transfer

            while (true) {
                currentChunk = chunkQueue.poll();
                if (currentChunk == null) break; // Finished all work

                if (!downloadChunk(dis, dos, currentChunk, buffer)) {
                    // Put back if it's a transient failure, then exit this worker
                    chunkQueue.add(currentChunk);
                    currentChunk = null; // Prevent re-addition in finally
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("[Worker " + sourceIndex + "] Source " + targetIp + " failed: " + e.getMessage());
            // If we crash before finishing a chunk, return it to the queue
            if (currentChunk != null) {
                chunkQueue.add(currentChunk);
            }
        } finally {
            running = false;
        }
    }

    private boolean downloadChunk(DataInputStream dis, DataOutputStream dos, int chunkIdx, byte[] buffer) {
        long offset = (long) chunkIdx * chunkSize;
        int length = (int) Math.min(chunkSize, fileSize - offset);

        try {
            // 1. Send Request
            dos.writeUTF(filename);
            dos.writeLong(offset);
            dos.writeInt(length);
            dos.flush();

            // 2. Read Server Status (1 = OK)
            if (dis.readInt() != 1) return false;

            // 3. Receive Data loop
            int totalRead = 0;
            while (totalRead < length) {
                int toRead = Math.min(buffer.length, length - totalRead);
                int bytesRead = dis.read(buffer, 0, toRead);
                if (bytesRead == -1) return false; // Stream closed unexpectedly

                // Positional thread-safe write to file
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);
                while (byteBuffer.hasRemaining()) {
                    fileChannel.write(byteBuffer, offset + totalRead);
                }

                totalRead += bytesRead;
                bytesDownloadedInSession += bytesRead;
                globalDownloadedCounter.addAndGet(bytesRead);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public long getTotalBytesCompleted() { return bytesDownloadedInSession; }
    public boolean isRunning() { return running; }
}
