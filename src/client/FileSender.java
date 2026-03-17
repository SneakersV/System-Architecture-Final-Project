package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.net.Socket;

/**
 * Thread responsible for sending a specific file fragment to a client.
 */
public class FileSender extends Thread {
    
    private Socket socket;
    private String sharedFolderPath;

    public FileSender(Socket socket, String sharedFolderPath) {
        this.socket = socket;
        this.sharedFolderPath = sharedFolderPath;
    }

    @Override
    public void run() {
        try {
            // Setup streams
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

            // Read the request details from the downloader
            String filename = dis.readUTF();
            long offset = dis.readLong();
            int lengthToRead = dis.readInt();

            System.out.println("[FileSender] Requested " + filename + " | Offset: " + offset + " | Length: " + lengthToRead);

            File targetFile = new File(sharedFolderPath, filename);
            if (!targetFile.exists()) {
                System.err.println("[FileSender] File not found: " + filename);
                dos.writeInt(-1); // Send error code
            } else {
                dos.writeInt(1); // Send success code

                // Open the file for reading at the specific offset using RandomAccessFile
                try (RandomAccessFile raf = new RandomAccessFile(targetFile, "r")) {
                    raf.seek(offset);

                    byte[] buffer = new byte[8192];
                    int totalRead = 0;
                    while (totalRead < lengthToRead) {
                        int remaining = lengthToRead - totalRead;
                        int readSize = Math.min(buffer.length, remaining);
                        int bytesRead = raf.read(buffer, 0, readSize);
                        
                        // EOF reached earlier than expected
                        if (bytesRead == -1) break;

                        dos.write(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                    dos.flush();
                    System.out.println("[FileSender] Finished sending " + totalRead + " bytes for " + filename);
                }
            }
        } catch (java.net.SocketException se) {
            // Handle client disconnection or "Connection reset" gracefully
            System.out.println("[FileSender] Client disconnected or canceled the download (" + se.getMessage() + ").");
        } catch (Exception e) {
            System.err.println("[FileSender] Unexpected Exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (Exception e) {
                // Ignore closing errors
            }
        }
    }
}
