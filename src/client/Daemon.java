package client;

import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.rmi.Naming;
import shared.ClientInfo;
import shared.Directory;

/**
 * The Daemon acts as a mini-server on each participating client.
 * It registers its available files to the Directory and opens a ServerSocket
 * to serve file fragments to downloading clients.
 */
public class Daemon {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java client.Daemon <Directory_IP> <Shared_Folder_Path>");
            return;
        }

        String directoryIp = args[0];
        String sharedFolderPath = args[1];

        try {
            // 1. Locate the Directory Server via RMI
            String rmiUrl = "rmi://" + directoryIp + ":1099/Directory";
            Directory directory = (Directory) Naming.lookup(rmiUrl);
            System.out.println("Connected to Directory Server at " + rmiUrl);

            // 2. Open a random free port for the TCP Socket server
            ServerSocket serverSocket = new ServerSocket(0);
            int localPort = serverSocket.getLocalPort();
            String localIp = InetAddress.getLocalHost().getHostAddress();
            ClientInfo myInfo = new ClientInfo(localIp, localPort);
            System.out.println("Daemon started at " + localIp + ":" + localPort);

            // 3. Scan the shared folder and register files
            File folder = new File(sharedFolderPath);
            if (!folder.exists() || !folder.isDirectory()) {
                System.err.println(
                        "The specified shared folder does not exist or is not a directory: " + sharedFolderPath);
                return;
            }

            File[] listOfFiles = folder.listFiles();
            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        System.out.println("Registering file: " + file.getName() + " (" + file.length() + " bytes)");
                        directory.registerFile(file.getName(), file.length(), myInfo);
                    }
                }
            }

            // 3.5. Background Thread to detect new files dynamically (Every 10 seconds)
            Thread folderScanner = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(5000); // 5 seconds
                        File[] currentFiles = folder.listFiles();
                        if (currentFiles != null) {
                            for (File file : currentFiles) {
                                if (file.isFile()) {
                                    // Directory.registerFile is idempotent, it won't add duplicate clients
                                    directory.registerFile(file.getName(), file.length(), myInfo);
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Scanner exception: " + e.getMessage());
                    }
                }
            });
            folderScanner.setDaemon(true); // Don't prevent JVM shutdown
            folderScanner.start();

            // 3.6. Background Heartbeat Thread (Every 5 seconds)
            Thread heartbeatThread = new Thread(() -> {
                while (true) {
                    try {
                        directory.heartBeat(myInfo);
                        Thread.sleep(5000);
                    } catch (Exception e) {
                        System.err.println("Heartbeat error: " + e.getMessage());
                        // Try to reconnect if server was down?
                    }
                }
            });
            heartbeatThread.setDaemon(true);
            heartbeatThread.start();

            // 3.7. Shutdown Hook for graceful unregistration
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    System.out.println("\nDaemon shutting down... Unregistering from Directory.");
                    directory.unregisterClient(myInfo);
                } catch (Exception e) {
                    System.err.println("Shutdown unregistration error: " + e.getMessage());
                }
            }));

            // 4. Listen for incoming download requests
            System.out.println("Daemon is waiting for download requests...");
            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Create a new thread to handle the fragment sending
                FileSender sender = new FileSender(clientSocket, sharedFolderPath);
                sender.start();
            }

        } catch (Exception e) {
            System.err.println("Daemon Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
