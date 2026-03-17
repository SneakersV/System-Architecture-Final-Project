package client;

import java.rmi.Naming;
import java.util.*;
import shared.ClientInfo;
import shared.Directory;
import shared.FileInfo;
import java.io.*;
import java.nio.channels.FileChannel;
import java.net.Socket;
import java.net.InetSocketAddress;

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
            System.out.println("File size: " + (fileSize / 1024.0 / 1024.0) + " MB. Reported sources: " + sources.size());

            // 1.5. SOURCE PROBING (Vetting)
            String localIp = System.getProperty("java.rmi.server.hostname");
            System.out.println("[System] Vetting sources... (Local IP: " + (localIp != null ? localIp : "unknown") + ")");
            List<ClientInfo> vettedSources = getVettedSources(sources, localIp);
            if (vettedSources.isEmpty()) {
                System.err.println("[Error] No valid remote sources available (sources are either offline or it's just you).");
                return;
            }
            int numSources = vettedSources.size();
            System.out.println("[System] " + numSources + "/" + sources.size() + " sources are alive and remote.");

            // 2. Prepare the local file using a temporary extension
            File downloadDir = new File(downloadFolderPath);
            if (!downloadDir.exists()) downloadDir.mkdirs();
            
            File finalFile = new File(downloadDir, filename);
            File partFile = new File(downloadDir, filename + ".part");
            File metaFile = new File(downloadDir, filename + ".part.meta");

            long[] savedProgress = new long[numSources];
            boolean isResume = false;

            if (finalFile.exists()) {
                System.out.println("Warning: " + filename + " already exists. Overwriting...");
            } else if (partFile.exists() && metaFile.exists()) {
                System.out.print("[System] Found partial download for '" + filename + "'. Resume? (y/n): ");
                Scanner sc = new Scanner(System.in);
                if (sc.hasNextLine()) {
                    String choice = sc.nextLine().trim().toLowerCase();
                    if (choice.equals("y") || choice.equals("yes")) {
                        savedProgress = loadMetadata(metaFile, fileSize, numSources);
                        if (savedProgress != null) {
                            isResume = true;
                            System.out.println("[System] Resuming download from saved progress...");
                        } else {
                            System.out.println("[System] Metadata mismatch (likely server changed sources). Starting fresh...");
                        }
                    }
                }
            }

            long startTime = System.currentTimeMillis();
            long totalTime = 0;
            Set<String> blacklist = new HashSet<>();

            // Create/truncate the part file to the exact size
            try (RandomAccessFile partRaf = new RandomAccessFile(partFile, "rw")) {
                if (!isResume) partRaf.setLength(fileSize);
                FileChannel sharedChannel = partRaf.getChannel();

                System.out.println("------------------------------------------------");
                System.out.println(isResume ? "RESUMING PARALLEL DOWNLOAD" : "INITIATING PARALLEL DOWNLOAD");
                System.out.println("File: " + filename);
                System.out.println("Sources Involved: " + numSources);
                System.out.println("------------------------------------------------");

                // 3. Calculate fragments and launch downloaders
                long fragmentSize = fileSize / numSources;
                FragmentDownloader[] downloaders = new FragmentDownloader[numSources];
                long[] fragmentStartTimes = new long[numSources];
                
                for (int i = 0; i < numSources; i++) {
                    ClientInfo source = vettedSources.get(i);
                    long baseOffset = i * fragmentSize;
                    int baseLength = (i == numSources - 1) ? (int) (fileSize - baseOffset) : (int) fragmentSize;
                    
                    long currentDownloaded = isResume ? savedProgress[i] : 0;
                    long offset = baseOffset + currentDownloaded;
                    int lengthToRead = (int) (baseLength - currentDownloaded);

                    if (lengthToRead > 0) {
                        System.out.println("[Control] Thread " + i + ": " + (isResume ? "Resuming at " : "Starting at ") + offset + " from " + source.getIp());
                        fragmentStartTimes[i] = System.currentTimeMillis();
                        downloaders[i] = new FragmentDownloader(source.getIp(), source.getPort(), filename, offset, lengthToRead, sharedChannel, i);
                        downloaders[i].setBaseInfo(baseOffset, baseLength, (int)currentDownloaded);
                        downloaders[i].start();
                    } else {
                        downloaders[i] = new FragmentDownloader(source.getIp(), source.getPort(), filename, offset, 0, sharedChannel, i);
                        downloaders[i].setBaseInfo(baseOffset, baseLength, baseLength);
                        downloaders[i].markSuccess();
                    }
                }

                // Metadata auto-save thread
                final FragmentDownloader[] fDownloaders = downloaders;
                Thread metaSaver = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(2000);
                            saveMetadata(metaFile, fileSize, fDownloaders);
                        } catch (InterruptedException e) { break; }
                        catch (Exception ignored) {}
                    }
                });
                metaSaver.setDaemon(true);
                metaSaver.start();

                // 4. Monitor and handle failures (Adaptive resume)
                boolean allDone = false;
                int[] retryCounts = new int[numSources];
                long[] performanceMetrics = new long[numSources];

                while (!allDone) {
                    allDone = true;
                    long totalDownloadedOverall = 0;
                    
                    for (int i = 0; i < numSources; i++) {
                        FragmentDownloader fd = downloaders[i];
                        totalDownloadedOverall += fd.getTotalBytesCompleted();
                        
                        if (fd.isAlive()) {
                            allDone = false;
                        } 
                        else if (!fd.isSuccess()) {
                            allDone = false; 
                            
                            retryCounts[i]++;
                            if (retryCounts[i] > 10) {
                                metaSaver.interrupt();
                                throw new Exception("Fragment " + i + " failed 10 times. Aborting.");
                            }

                            // Blacklist the source if it failed twice
                            if (retryCounts[i] >= 2) {
                                blacklist.add(vettedSources.get(fd.getSourceIndex()).getIp());
                            }

                            int downloadedThisTurn = fd.getBytesDownloaded();
                            long newOffset = fd.getOffset() + downloadedThisTurn;
                            int newLength = fd.getLength() - downloadedThisTurn;

                            int bestSourceIdx = -1;
                            long bestRate = -1;
                            for (int s = 0; s < numSources; s++) {
                                if (blacklist.contains(vettedSources.get(s).getIp())) continue;
                                if (performanceMetrics[s] > bestRate) {
                                    bestRate = performanceMetrics[s];
                                    bestSourceIdx = s;
                                }
                            }
                            if (bestSourceIdx == -1) {
                                for (int s = 0; s < numSources; s++) {
                                    if (!blacklist.contains(vettedSources.get(s).getIp())) {
                                        bestSourceIdx = s;
                                        break;
                                    }
                                }
                            }
                            
                            if (bestSourceIdx == -1) throw new Exception("All sources failed or blacklisted for Fragment " + i);
                            
                            ClientInfo rs = vettedSources.get(bestSourceIdx);
                            System.err.println("\n[Recovery] Thread " + i + " failed. Migrating " + newLength + " bytes to " + rs.getIp());
                            
                            fragmentStartTimes[i] = System.currentTimeMillis();
                            int previouslyPersisted = fd.getPreviouslyDownloaded() + downloadedThisTurn;
                            downloaders[i] = new FragmentDownloader(rs.getIp(), rs.getPort(), filename, newOffset, newLength, sharedChannel, bestSourceIdx);
                            downloaders[i].setBaseInfo(fd.getBaseOffset(), fd.getBaseLength(), previouslyPersisted);
                            downloaders[i].start();
                        } else {
                            long duration = System.currentTimeMillis() - fragmentStartTimes[i];
                            if (duration > 0) {
                                performanceMetrics[fd.getSourceIndex()] = fd.getLength() / duration;
                            }
                        }
                    }

                    printProgressBar(totalDownloadedOverall, fileSize, startTime);
                    if (!allDone) Thread.sleep(200);
                }
                System.out.println();
                metaSaver.interrupt();

                totalTime = System.currentTimeMillis() - startTime;
                sharedChannel.force(true); 
            }

            // 5. Finalizing
            if (partFile.renameTo(finalFile)) {
                metaFile.delete(); 
                System.out.println("------------------------------------------------");
                System.out.println("DOWNLOAD SUCCESSFUL!");
                System.out.println("Total Time: " + (totalTime / 1000.0) + " seconds");
                System.out.println("Saved to: " + finalFile.getAbsolutePath());
                System.out.println("------------------------------------------------");
            } else {
                throw new Exception("Failed to finalize file.");
            }

        } catch (Exception e) {
            System.err.println("Download error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static List<ClientInfo> getVettedSources(List<ClientInfo> rawSources, String localIp) {
        List<ClientInfo> vetted = new ArrayList<>();
        for (ClientInfo s : rawSources) {
            // Skip self-source to avoid "File not found" on partial files
            if (localIp != null && s.getIp().equals(localIp)) {
                continue;
            }
            try (Socket testSocket = new Socket()) {
                testSocket.connect(new InetSocketAddress(s.getIp(), s.getPort()), 1000); // 1s timeout
                vetted.add(s);
            } catch (Exception e) {
                System.err.println("[System] Source " + s.getIp() + ":" + s.getPort() + " is unreachable. Skipping.");
            }
        }
        return vetted;
    }

    private static void saveMetadata(File metaFile, long fileSize, FragmentDownloader[] downloaders) {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(metaFile))) {
            dos.writeLong(fileSize);
            dos.writeInt(downloaders.length);
            for (FragmentDownloader fd : downloaders) {
                dos.writeLong(fd.getTotalBytesCompleted());
            }
        } catch (Exception ignored) {}
    }

    private static long[] loadMetadata(File metaFile, long fileSize, int numSources) {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(metaFile))) {
            if (dis.readLong() != fileSize) return null;
            if (dis.readInt() != numSources) return null;
            long[] progress = new long[numSources];
            for (int i = 0; i < numSources; i++) {
                progress[i] = dis.readLong();
            }
            return progress;
        } catch (Exception e) {
            return null;
        }
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
