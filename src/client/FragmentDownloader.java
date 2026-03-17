package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;

/**
 * Thread that connects to a specific Daemon and downloads a specific part of a file.
 */
public class FragmentDownloader extends Thread {

    private String targetIp;
    private int targetPort;
    private String filename;
    private long offset;
    private int length;
    private String outputFilePath;
    private int sourceIndex;
    
    private boolean success = false;
    private int bytesDownloaded = 0;

    public FragmentDownloader(String targetIp, int targetPort, String filename, long offset, int length, String outputFilePath, int sourceIndex) {
        this.targetIp = targetIp;
        this.targetPort = targetPort;
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.outputFilePath = outputFilePath;
        this.sourceIndex = sourceIndex;
    }

    public boolean isSuccess() { return success; }
    public int getBytesDownloaded() { return bytesDownloaded; }
    public long getOffset() { return offset; }
    public int getLength() { return length; }
    public int getSourceIndex() { return sourceIndex; }

    @Override
    public void run() {
        Socket socket = null;
        try {
            // 1. Establish TCP connection to the specific Daemon
            socket = new Socket(targetIp, targetPort);
            
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(socket.getInputStream());

            // 2. Request the fragment
            dos.writeUTF(filename);
            dos.writeLong(offset);
            dos.writeInt(length);
            dos.flush();

            // 3. Verify success status from server
            int status = dis.readInt();
            if (status == -1) {
                System.err.println("[FragmentDownloader] Error: Daemon could not find the file.");
                return;
            }

            // 4. Read bytes and write them to the specific offset in the local file
            // Each thread gets its own RandomAccessFile instance pointing to the same file 
            // to avoid file pointer conflicts between threads.
            try (RandomAccessFile raf = new RandomAccessFile(outputFilePath, "rw")) {
                raf.seek(offset);

                byte[] buffer = new byte[8192];
                int totalRead = 0;
                
                while (totalRead < length) {
                    int remaining = length - totalRead;
                    int readSize = Math.min(buffer.length, remaining);
                    
                    int bytesRead = dis.read(buffer, 0, readSize);
                    if (bytesRead == -1) {
                        System.err.println("[FragmentDownloader] Error: Connection closed prematurely by Daemon.");
                        break;
                    }

                    raf.write(buffer, 0, bytesRead);
                    totalRead += bytesRead;
                    this.bytesDownloaded = totalRead;
                }
                
                if (totalRead == length) {
                    System.out.println("[Thread " + sourceIndex + "] Finished fragment [" + offset + " -> " + (offset + length) + "] from " + targetIp + " (" + totalRead + " bytes)");
                    this.success = true;
                }
            }

        } catch (Exception e) {
            System.err.println("[FragmentDownloader] Exception while downloading from " + targetIp + ":" + targetPort + " -> " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (Exception e) {
                // Ignore
            }
        }
    }
}
