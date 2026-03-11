import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Directory extends UnicastRemoteObject implements DirectoryInterface {

    // filename -> list of clients who have the file
    private HashMap<String, List<ClientInfo>> fileRegistry;
    // client -> list of files it owns
    private HashMap<ClientInfo, List<String>> clientFiles;

    public Directory() throws RemoteException {
        super();
        this.fileRegistry = new HashMap<>();
        this.clientFiles = new HashMap<>();
    }

    public synchronized void registerClient(ClientInfo client, List<String> files) throws RemoteException {
        System.out.println("Registering client: " + client + " with files: " + files);
        // Remove old registration if client reconnects
        unregisterClient(client);
        clientFiles.put(client, new ArrayList<>(files));
        for (String file : files) {
            if (!fileRegistry.containsKey(file)) {
                fileRegistry.put(file, new ArrayList<>());
            }
            fileRegistry.get(file).add(client);
        }
    }

    public synchronized void unregisterClient(ClientInfo client) throws RemoteException {
        List<String> files = clientFiles.remove(client);
        if (files != null) {
            System.out.println("Unregistering client: " + client);
            for (String file : files) {
                List<ClientInfo> clients = fileRegistry.get(file);
                if (clients != null) {
                    clients.remove(client);
                    if (clients.isEmpty()) {
                        fileRegistry.remove(file);
                    }
                }
            }
        }
    }

    public synchronized List<ClientInfo> lookupFile(String filename) throws RemoteException {
        System.out.println("Lookup for file: " + filename);
        List<ClientInfo> clients = fileRegistry.get(filename);
        if (clients == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(clients);
    }

    public static void main(String[] args) {
        try {
            // Launch rmiregistry within the JVM (like slide 25 of RMI lesson)
            LocateRegistry.createRegistry(1099);

            // Create an instance of the server object
            Directory dir = new Directory();

            // Register with the naming service
            Naming.rebind("//localhost:1099/DirectoryService", dir);
            System.out.println("Directory RMI Server is running on port 1099...");

        } catch (Exception e) {
            System.err.println("Directory exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
