package client;

import java.util.Scanner;

/**
 * ClientNode is the unified Peer-to-Peer agent.
 * It runs a background Daemon service to share files and provides an
 * interactive CLI to download files from other nodes.
 */
public class ClientNode {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java client.ClientNode <Directory_IP> <Shared_Folder_Path>");
            return;
        }

        String directoryIp = args[0];
        String sharedFolderPath = args[1];

        // 1. Start the Daemon backend in a separate thread
        Thread daemonThread = new Thread(() -> {
            System.out.println("[System] Starting background Daemon service...");
            Daemon.runService(directoryIp, sharedFolderPath);
        });
        daemonThread.start();

        // 2. Provide an interactive CLI for downloading
        Scanner scanner = new Scanner(System.in);
        System.out.println("\n================================================");
        System.out.println("   WELCOME TO SONGSONG P2P INTEGRATED NODE");
        System.out.println("================================================");
        System.out.println("Commands:");
        System.out.println("  download <filename> : Download a file from the network");
        System.out.println("  exit                : Stop the node");
        System.out.println("================================================");

        while (true) {
            System.out.print("\nP2P> ");
            String input = scanner.nextLine().trim();

            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Exiting... Background services will stop.");
                System.exit(0);
            }

            if (input.startsWith("download ")) {
                String filename = input.substring(9).trim();
                if (filename.isEmpty()) {
                    System.out.println("Please specify a filename.");
                    continue;
                }

                System.out.println("[System] Initiating download for: " + filename);
                // We use our shared folder path as the download destination 
                // so that the Daemon part can immediately seed it.
                Download.executeDownload(filename, directoryIp, sharedFolderPath);
            } else if (!input.isEmpty()) {
                System.out.println("Unknown command. Use 'download <filename>' or 'exit'.");
            }
        }
    }
}
