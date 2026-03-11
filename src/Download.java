import java.rmi.*;
import java.net.*;
import java.io.*;
import java.util.List;

public class Download {

    private static final int CHUNK_SIZE = 1024 * 1024; // 1 MB

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Download <filename> [directoryHost]");
            return;
        }
        String filename = args[0];
        String directoryHost = args.length > 1 ? args[1] : "localhost";

        try {
            // 1. Lookup Directory via RMI (Naming.lookup like in the lesson)
            DirectoryInterface dir = (DirectoryInterface) Naming.lookup(
                "//" + directoryHost + ":1099/DirectoryService");

            List<ClientInfo> clients = dir.lookupFile(filename);
            if (clients == null || clients.isEmpty()) {
                System.out.println("File not found on any client.");
                return;
            }
            System.out.println("Found " + clients.size() + " source(s): " + clients);

            // 2. Get file size from the first available client
            long fileSize = getFileSize(clients.get(0), filename);
            if (fileSize <= 0) {
                System.out.println("Cannot get file size.");
                return;
            }
            System.out.println("File size: " + fileSize + " bytes");

            // 3. Prepare output file
            File outputFile = new File("downloaded_" + filename);
            RandomAccessFile raf = new RandomAccessFile(outputFile, "rw");
            raf.setLength(fileSize);
            raf.close();

            // 4. Calculate chunks
            int numChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
            System.out.println("Splitting into " + numChunks + " chunk(s)");

            // 5. Create download threads (Thread[] + start/join, like lesson pattern)
            long startTime = System.currentTimeMillis();

            DownloadThread[] threads = new DownloadThread[numChunks];
            for (int i = 0; i < numChunks; i++) {
                long offset = (long) i * CHUNK_SIZE;
                int length = (int) Math.min(CHUNK_SIZE, fileSize - offset);
                // Round-robin: each chunk starts with a different client
                int clientIndex = i % clients.size();
                threads[i] = new DownloadThread(
                    clients, clientIndex, filename, offset, length, outputFile);
                threads[i].start();
            }

            // Wait for all threads to finish (join pattern)
            boolean allOk = true;
            for (int i = 0; i < numChunks; i++) {
                threads[i].join();
                if (!threads[i].success) allOk = false;
            }

            long elapsed = System.currentTimeMillis() - startTime;

            if (allOk) {
                System.out.println("Download complete in " + elapsed + " ms");
            } else {
                System.out.println("Download had errors.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** Ask one Daemon for the file size using the text protocol */
    static long getFileSize(ClientInfo client, String filename) {
        try {
            Socket s = new Socket(client.getIp(), client.getPort());
            OutputStream out = s.getOutputStream();
            BufferedReader in = new BufferedReader(
                new InputStreamReader(s.getInputStream()));

            out.write(("GETSIZE " + filename + "\n").getBytes());
            out.flush();

            String line = in.readLine();
            in.close();
            out.close();
            s.close();
            return Long.parseLong(line);
        } catch (Exception e) {
            System.err.println("getFileSize error: " + e.getMessage());
            return -1;
        }
    }
}

/**
 * Thread that downloads one chunk from a specific Daemon.
 * On failure, it retries with the next client in the list (failure handling).
 * Pattern: extends Thread (like Slave / LoadBalancer in socket lesson).
 */
class DownloadThread extends Thread {
    private List<ClientInfo> clients;
    private int startIndex;
    private String filename;
    private long offset;
    private int length;
    private File outputFile;
    boolean success = false;

    public DownloadThread(List<ClientInfo> clients, int startIndex,
                          String filename, long offset, int length, File outputFile) {
        this.clients = clients;
        this.startIndex = startIndex;
        this.filename = filename;
        this.offset = offset;
        this.length = length;
        this.outputFile = outputFile;
    }

    public void run() {
        // Try each client in round-robin order (failure handling)
        for (int attempt = 0; attempt < clients.size(); attempt++) {
            ClientInfo client = clients.get((startIndex + attempt) % clients.size());
            try {
                Socket s = new Socket(client.getIp(), client.getPort());
                OutputStream out = s.getOutputStream();
                InputStream in = s.getInputStream();

                // Send request (text protocol like HTTP in Comanche)
                out.write(("GET " + filename + " " + offset + " " + length + "\n").getBytes());
                out.flush();

                // Read the header line byte-by-byte (avoid BufferedReader
                // which would buffer ahead and consume binary payload)
                String header = readLine(in);
                int bytesToRead = Integer.parseInt(header.trim());

                if (bytesToRead <= 0) {
                    s.close();
                    continue; // try next client
                }

                // Read the raw bytes
                byte[] buffer = new byte[bytesToRead];
                int totalRead = 0;
                while (totalRead < bytesToRead) {
                    int r = in.read(buffer, totalRead, bytesToRead - totalRead);
                    if (r == -1) break;
                    totalRead += r;
                }

                // Write to output file at the correct offset
                RandomAccessFile raf = new RandomAccessFile(outputFile, "rw");
                raf.seek(offset);
                raf.write(buffer, 0, totalRead);
                raf.close();

                s.close();

                System.out.println("Chunk offset=" + offset + " len=" + totalRead
                    + " from " + client);
                success = true;
                return;

            } catch (Exception e) {
                System.err.println("Error from " + client + ": " + e.getMessage()
                    + " — retrying...");
            }
        }
        System.err.println("All clients failed for chunk at offset " + offset);
    }

    /** Read one line from InputStream byte-by-byte (no buffering ahead) */
    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') break;
            if (c != '\r') sb.append((char) c);
        }
        return sb.toString();
    }
}
