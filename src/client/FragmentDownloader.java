package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A worker thread that connects to a specific Daemon and processes chunks from a shared queue.
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
    private int chunksCompleted = 0;
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

    public int getSourceIndex() { return sourceIndex; }
    public boolean isSuccess() { return !running; } // If it finished its loop naturally
    public long getBytesDownloaded() { return bytesDownloadedInSession; }
    public int getChunksCompleted() { return chunksCompleted; }

    @Override
    public void run() {
        try {
            while (true) {
                Integer chunkIdx = chunkQueue.poll();
                if (chunkIdx == null) break; // No more work

                if (!downloadChunk(chunkIdx)) {
                    // Put back for retry by another thread if this source failed
                    chunkQueue.add(chunkIdx);
                    throw new Exception("Source failed during chunk " + chunkIdx);
                }
                chunksCompleted++;
            }
        } catch (Exception e) {
            System.err.println("[Worker " + sourceIndex + "] Stopped: " + e.getMessage());
        } finally {
            running = false;
        }
    }

    private boolean downloadChunk(int chunkIdx) {
        long offset = (long) chunkIdx * chunkSize;
        int length = (int) Math.min(chunkSize, fileSize - offset);

        try (Socket socket = new Socket(targetIp, targetPort)) {
            socket.setSoTimeout(5000); // 5s read timeout
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            // 1. Request
            dos.writeUTF(filename);
            dos.writeLong(offset);
            dos.writeInt(length);
            dos.flush();

            // 2. Status
            if (dis.readInt() == -1) return false;

            // 3. Data Transfer
            byte[] buffer = new byte[8192];
            int totalRead = 0;
            while (totalRead < length) {
                int bytesRead = dis.read(buffer, 0, Math.min(buffer.length, length - totalRead));
                if (bytesRead == -1) return false;

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

    // Compatibility methods for Download.java during transition
    public long getTotalBytesCompleted() { return bytesDownloadedInSession; } // This now represents session progress
    public void markSuccess() { running = false; }
}
