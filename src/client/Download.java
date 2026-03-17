package client;

import java.rmi.Naming;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import shared.ClientInfo;
import shared.Directory;
import shared.FileInfo;
import java.io.*;
import java.nio.channels.FileChannel;
import java.net.Socket;
import java.net.InetSocketAddress;

/**
 * The Download client using Dynamic Chunk-Based Load Balancing.
 */
public class Download {

    private static final int CHUNK_SIZE = 1024 * 1024; // 1MB chunks

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java client.Download <filename> <Directory_IP> <Download_Folder_Path>");
            return;
        }

        String filename = args[0];
        String directoryIp = args[1];
        String downloadFolderPath = args[2];

        executeDownload(filename, directoryIp, downloadFolderPath);
    }

    public static void executeDownload(String filename, String directoryIp, String downloadFolderPath) {
        try {
            // 1. Connect to Directory to get file info
            String rmiUrl = "rmi://" + directoryIp + ":1099/Directory";
            Directory directory = (Directory) Naming.lookup(rmiUrl);
            System.out.println("Connected to Directory at " + rmiUrl);

            FileInfo fileInfo = directory.lookupFile(filename);
            if (fileInfo == null) {
                System.out.println("File not found in the directory.");
                return;
            }

            List<ClientInfo> sources = fileInfo.getClients();
            if (sources.isEmpty()) {
                System.out.println("No clients are currently hosting this file.");
                return;
            }

            long fileSize = fileInfo.getFileSize();
            int totalChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);
            System.out.println("File size: " + (fileSize / 1024.0 / 1024.0) + " MB. Chunks: " + totalChunks);

            // 1.5. SOURCE PROBING (Vetting)
            String localIp = System.getProperty("java.rmi.server.hostname");
            List<ClientInfo> vettedSources = getVettedSources(sources, localIp);
            if (vettedSources.isEmpty()) {
                System.err.println("[Error] No valid remote sources available.");
                return;
            }
            int numWorkers = vettedSources.size();
            System.out.println("[System] Launching " + numWorkers + " parallel workers.");

            // 2. Prepare files
            File downloadDir = new File(downloadFolderPath);
            if (!downloadDir.exists())
                downloadDir.mkdirs();

            File finalFile = new File(downloadDir, filename);
            File partFile = new File(downloadDir, filename + ".part");

            if (finalFile.exists()) {
                System.out.println("Warning: " + filename + " already exists. Overwriting...");
            }

            // 3. Setup Work Queue and Workers
            ConcurrentLinkedQueue<Integer> chunkQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < totalChunks; i++) {
                chunkQueue.add(i);
            }

            AtomicLong globalDownloaded = new AtomicLong(0);
            long startTime = System.currentTimeMillis();

            try (RandomAccessFile partRaf = new RandomAccessFile(partFile, "rw")) {
                partRaf.setLength(fileSize);
                FileChannel sharedChannel = partRaf.getChannel();

                FragmentDownloader[] workers = new FragmentDownloader[numWorkers];
                for (int i = 0; i < numWorkers; i++) {
                    ClientInfo s = vettedSources.get(i);
                    workers[i] = new FragmentDownloader(s.getIp(), s.getPort(), filename, sharedChannel, i,
                            chunkQueue, CHUNK_SIZE, fileSize, globalDownloaded);
                    workers[i].start();
                }

                // Monitoring loop
                while (true) {
                    boolean anyAlive = false;
                    for (FragmentDownloader w : workers) {
                        if (w.isAlive())
                            anyAlive = true;
                    }

                    printProgressBar(globalDownloaded.get(), fileSize, startTime);

                    if (!anyAlive && chunkQueue.isEmpty())
                        break;
                    if (!anyAlive && !chunkQueue.isEmpty()) {
                        System.err.println("\n[Error] All workers died but work remains.");
                        break;
                    }
                    Thread.sleep(200);
                }

                System.out.println();
                sharedChannel.force(true);
            }

            // 4. Cleanup and Finish
            long totalTime = System.currentTimeMillis() - startTime;
            finalizeFile(partFile, finalFile, totalTime, fileSize);

        } catch (Exception e) {
            System.err.println("Download error: " + e.getMessage());
        }
    }

    private static void finalizeFile(File part, File finalF, long totalTimeMs, long totalSizeBytes) throws Exception {
        if (part.renameTo(finalF)) {
            System.out.println("\n------------------------------------------------");
            System.out.println("DOWNLOAD SUCCESSFUL!");
            System.out.println(String.format("Total Time: %.2f seconds", totalTimeMs / 1000.0));
            System.out.println(String.format("Total Size: %.2f MB", totalSizeBytes / 1024.0 / 1024.0));
            System.out.println("Saved to: " + finalF.getAbsolutePath());
            System.out.println("------------------------------------------------");
        } else {
            throw new Exception("Failed to rename .part file.");
        }
    }

    private static List<ClientInfo> getVettedSources(List<ClientInfo> rawSources, String localIp) {
        List<ClientInfo> vetted = new ArrayList<>();
        for (ClientInfo s : rawSources) {
            if (localIp != null && s.getIp().equals(localIp))
                continue;
            try (Socket testSocket = new Socket()) {
                testSocket.connect(new InetSocketAddress(s.getIp(), s.getPort()), 1000); // 1s timeout
                vetted.add(s);
            } catch (Exception e) {
                System.err.println("[System] Source " + s.getIp() + ":" + s.getPort() + " unreachable.");
            }
        }
        return vetted;
    }

    private static void printProgressBar(long current, long total, long startTime) {
        int width = 40;
        double progress = (double) current / total;
        int completedWidth = (int) (progress * width);

        StringBuilder sb = new StringBuilder("\rProgress: [");
        for (int i = 0; i < width; i++) {
            if (i < completedWidth)
                sb.append("=");
            else if (i == completedWidth)
                sb.append(">");
            else
                sb.append(" ");
        }

        long elapsed = System.currentTimeMillis() - startTime;
        double speed = (elapsed > 0) ? (current / 1024.0 / 1024.0) / (elapsed / 1000.0) : 0;

        sb.append(String.format("] %d%% (%.2f MB/s)", (int) (progress * 100), speed));
        System.out.print(sb.toString());
    }
}
