import java.rmi.*;
import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Daemon {

    private int port;
    private String directoryHost;
    private String sharedFolder;

    public Daemon(int port, String directoryHost, String sharedFolder) {
        this.port = port;
        this.directoryHost = directoryHost;
        this.sharedFolder = sharedFolder;
    }

    public void start() {
        try {
            // 1. Scan shared folder for available files
            File folder = new File(sharedFolder);
            if (!folder.exists()) folder.mkdirs();
            File[] listOfFiles = folder.listFiles();
            List<String> files = new ArrayList<>();
            if (listOfFiles != null) {
                for (File f : listOfFiles) {
                    if (f.isFile()) files.add(f.getName());
                }
            }

            // 2. Register with Directory via RMI (Naming.lookup like in the lesson)
            DirectoryInterface dir = (DirectoryInterface) Naming.lookup(
                "//" + directoryHost + ":1099/DirectoryService");
            ClientInfo myInfo = new ClientInfo(
                InetAddress.getLocalHost().getHostAddress(), port);
            dir.registerClient(myInfo, files);
            System.out.println("Daemon registered with files: " + files);

            // Graceful unregister on shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { dir.unregisterClient(myInfo); } catch (Exception e) {}
            }));

            // 3. Start TCP server (pattern: Comanche.java from socket lesson)
            ServerSocket ss = new ServerSocket(port);
            System.out.println("Daemon listening on port " + port);
            while (true) {
                new DaemonWorker(ss.accept(), sharedFolder).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int port = 8081;
        String directoryHost = "localhost";
        String sharedFolder = "shared_files";
        if (args.length > 0) port = Integer.parseInt(args[0]);
        if (args.length > 1) directoryHost = args[1];
        if (args.length > 2) sharedFolder = args[2];
        new Daemon(port, directoryHost, sharedFolder).start();
    }
}

/**
 * Worker thread for each incoming connection.
 * Pattern: extends Thread, like Slave in the socket lesson.
 *
 * Protocol (text-based, like HTTP in Comanche):
 *   Client sends one line:
 *     "GETSIZE filename"       -> Server replies with file size as a text line
 *     "GET filename offset length" -> Server replies with raw bytes of that fragment
 */
class DaemonWorker extends Thread {
    private Socket socket;
    private String sharedFolder;

    public DaemonWorker(Socket socket, String sharedFolder) {
        this.socket = socket;
        this.sharedFolder = sharedFolder;
    }

    public void run() {
        try {
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
            OutputStream out = socket.getOutputStream();

            String request = in.readLine();
            System.out.println("Request: " + request);

            if (request == null) {
                socket.close();
                return;
            }

            String[] parts = request.split(" ");

            if (parts[0].equals("GETSIZE")) {
                // GETSIZE filename
                String filename = parts[1];
                File f = new File(sharedFolder, filename);
                if (f.exists()) {
                    out.write((f.length() + "\n").getBytes());
                } else {
                    out.write("-1\n".getBytes());
                }

            } else if (parts[0].equals("GET")) {
                // GET filename offset length
                String filename = parts[1];
                long offset = Long.parseLong(parts[2]);
                int length = Integer.parseInt(parts[3]);
                File f = new File(sharedFolder, filename);

                if (f.exists()) {
                    RandomAccessFile raf = new RandomAccessFile(f, "r");
                    raf.seek(offset);
                    byte[] buffer = new byte[length];
                    int bytesRead = raf.read(buffer);
                    raf.close();
                    if (bytesRead > 0) {
                        // First line: number of bytes that follow
                        out.write((bytesRead + "\n").getBytes());
                        out.write(buffer, 0, bytesRead);
                    } else {
                        out.write("0\n".getBytes());
                    }
                } else {
                    out.write("-1\n".getBytes());
                }
            }

            out.close();
            socket.close();
        } catch (Exception e) {
            System.err.println("DaemonWorker error: " + e.getMessage());
        }
    }
}
