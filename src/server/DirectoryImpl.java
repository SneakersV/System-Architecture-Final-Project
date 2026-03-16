package server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import shared.ClientInfo;
import shared.Directory;
import shared.FileInfo;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implementation of the RMI Directory service.
 */
public class DirectoryImpl extends UnicastRemoteObject implements Directory {

    private static final long serialVersionUID = 1L;

    // Stores the files registered by their filenames. 
    // ConcurrentHashMap allows safe multi-threaded access via RMI.
    private ConcurrentHashMap<String, FileInfo> directoryCache;
    // Map to track the last heartbeat from each client
    private ConcurrentHashMap<ClientInfo, Long> clientHeartbeats;
    
    private static final long HEARTBEAT_TIMEOUT = 15000; // 15 seconds

    public DirectoryImpl() throws RemoteException {
        super();
        this.directoryCache = new ConcurrentHashMap<>();
        this.clientHeartbeats = new ConcurrentHashMap<>();
        
        // Start a cleanup task to remove dead clients periodically
        Timer cleanupTimer = new Timer(true);
        cleanupTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                cleanupDeadClients();
            }
        }, HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT);
    }

    @Override
    public synchronized void registerFile(String filename, long fileSize, ClientInfo client) throws RemoteException {
        // If the file is not in our cache, add it
        FileInfo fileInfo = directoryCache.get(filename);
        if (fileInfo == null) {
            fileInfo = new FileInfo(filename, fileSize);
            directoryCache.put(filename, fileInfo);
            System.out.println("New file registered: " + filename + " (size: " + fileSize + " bytes)");
        }

        // Add the client to the list of hosts for this file
        fileInfo.addClient(client);
        System.out.println("Client " + client + " registered for file " + filename);
    }

    @Override
    public FileInfo lookupFile(String filename) throws RemoteException {
        FileInfo info = directoryCache.get(filename);
        if (info != null) {
            System.out.println("Lookup requested for " + filename + " -> Found " + info.getClients().size() + " sources.");
        } else {
            System.out.println("Lookup requested for " + filename + " -> Not found.");
        }
        return info;
    }

    @Override
    public synchronized void heartBeat(ClientInfo client) throws RemoteException {
        clientHeartbeats.put(client, System.currentTimeMillis());
        // System.out.println("Heartbeat received from " + client);
    }

    @Override
    public synchronized void unregisterClient(ClientInfo client) throws RemoteException {
        removeClientEverywhere(client);
    }

    private void cleanupDeadClients() {
        long now = System.currentTimeMillis();
        for (Map.Entry<ClientInfo, Long> entry : clientHeartbeats.entrySet()) {
            if (now - entry.getValue() > HEARTBEAT_TIMEOUT) {
                ClientInfo deadClient = entry.getKey();
                System.out.println("Client timeout: " + deadClient + ". Removing from directory.");
                removeClientEverywhere(deadClient);
            }
        }
    }

    private synchronized void removeClientEverywhere(ClientInfo client) {
        clientHeartbeats.remove(client);
        // Remove client from all FileInfo entries
        Iterator<FileInfo> it = directoryCache.values().iterator();
        while (it.hasNext()) {
            FileInfo info = it.next();
            info.getClients().remove(client);
            if (info.getClients().isEmpty()) {
                // Optional: remove file if no one has it
                // it.remove(); 
            }
        }
    }
}
