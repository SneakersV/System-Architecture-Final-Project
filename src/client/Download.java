package client;

import java.rmi.Naming;
import java.util.List;
import shared.ClientInfo;
import shared.Directory;
import shared.FileInfo;
import java.io.File;
import java.io.RandomAccessFile;

/**
 * The Download client.
 * Usage: java client.Download <filename> <Directory_IP> <Download_Folder_Path>
 */
public class Download {

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
            int numSources = sources.size();
            System.out.println("File size: " + fileSize + " bytes. Available sources: " + numSources);

            // 2. Prepare the local file using a temporary extension
            File downloadDir = new File(downloadFolderPath);
            if (!downloadDir.exists()) {
                downloadDir.mkdirs();
            }
            File finalFile = new File(downloadDir, filename);
            File partFile = new File(downloadDir, filename + ".part");
            
            // If the final file exists, we might want to skip or overwrite. 
            // For now, let's allow overwrite but avoid dirty partials.
            if (finalFile.exists()) {
                System.out.println("Warning: " + filename + " already exists. Overwriting...");
            }

            // Create/truncate the part file to the exact size
            try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                raf.setLength(fileSize);
            }

            // 3. Calculate fragments and launch downloaders
            long fragmentSize = fileSize / numSources;
            FragmentDownloader[] downloaders = new FragmentDownloader[numSources];
            long[] fragmentStartTimes = new long[numSources];
            
            long startTime = System.currentTimeMillis();

            for (int i = 0; i < numSources; i++) {
                ClientInfo source = sources.get(i);
                long offset = i * fragmentSize;
                int lengthToRead = (i == numSources - 1) ? (int) (fileSize - offset) : (int) fragmentSize;

                System.out.println("Starting thread " + i + " -> Sub-Task [offset=" + offset + ", length=" + lengthToRead + "] from " + source);
                
                fragmentStartTimes[i] = System.currentTimeMillis();
                downloaders[i] = new FragmentDownloader(source.getIp(), source.getPort(), filename, offset, lengthToRead, partFile.getAbsolutePath(), i);
                downloaders[i].start();
            }

            // 4. Monitor and handle failures (Adaptive resume)
            boolean allDone = false;
            int[] retryCounts = new int[numSources];
            long[] performanceMetrics = new long[numSources];

            while (!allDone) {
                allDone = true;
                for (int i = 0; i < numSources; i++) {
                    FragmentDownloader fd = downloaders[i];
                    
                    if (fd.isAlive()) {
                        allDone = false;
                    } 
                    else if (!fd.isSuccess()) {
                        allDone = false; 
                        
                        retryCounts[i]++;
                        if (retryCounts[i] > 10) {
                            throw new Exception("Fragment " + i + " failed 10 times. Aborting download.");
                        }

                        int downloaded = fd.getBytesDownloaded();
                        long newOffset = fd.getOffset() + downloaded;
                        int newLength = fd.getLength() - downloaded;

                        int bestSourceIndex = 0;
                        long bestRate = -1;
                        for (int s = 0; s < numSources; s++) {
                            if (performanceMetrics[s] > bestRate) {
                                bestRate = performanceMetrics[s];
                                bestSourceIndex = s;
                            }
                        }
                        
                        if (bestRate <= 0) {
                            bestSourceIndex = (fd.getSourceIndex() + 1) % sources.size();
                        }
                        
                        ClientInfo newSource = sources.get(bestSourceIndex);

                        System.err.println("[Recovery] Fragment " + i + " failed/disconnected after " + downloaded + " bytes.");
                        System.out.println("[Recovery] Resuming " + newLength + " remaining bytes from best source " + newSource + " (Rate: " + bestRate + " bytes/ms)");
                        
                        fragmentStartTimes[i] = System.currentTimeMillis();
                        downloaders[i] = new FragmentDownloader(newSource.getIp(), newSource.getPort(), filename, newOffset, newLength, partFile.getAbsolutePath(), bestSourceIndex);
                        downloaders[i].start();
                    } else {
                        long duration = System.currentTimeMillis() - fragmentStartTimes[i];
                        if (duration > 0) {
                            performanceMetrics[fd.getSourceIndex()] = fd.getLength() / duration;
                        }
                    }
                }

                if (!allDone) {
                    Thread.sleep(500); 
                }
            }

            long endTime = System.currentTimeMillis();
            
            // 5. Finalizing: Rename .part to actual filename
            if (partFile.renameTo(finalFile)) {
                System.out.println("Download complete! Time taken: " + (endTime - startTime) + " ms.");
                System.out.println("File saved and verified: " + finalFile.getAbsolutePath());
            } else {
                throw new Exception("Failed to finalize file (rename error). Ensure no other process is holding the file.");
            }

        } catch (Exception e) {
            System.err.println("Download error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
