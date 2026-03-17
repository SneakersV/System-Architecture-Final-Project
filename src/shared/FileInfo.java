package shared;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * Stores information about a file, including its size and the list of clients
 * that currently have it.
 * Must implement Serializable to be sent over RMI.
 */
public class FileInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String filename;
    private long fileSize;
    private CopyOnWriteArrayList<ClientInfo> clients;

    public FileInfo(String filename, long fileSize) {
        this.filename = filename;
        this.fileSize = fileSize;
        this.clients = new CopyOnWriteArrayList<>();
    }

    public String getFilename() {
        return filename;
    }

    public long getFileSize() {
        return fileSize;
    }

    public List<ClientInfo> getClients() {
        return clients;
    }

    public void addClient(ClientInfo client) {
        // CopyOnWriteArrayList.addIfAbsent is perfect here, but let's keep the logic clear
        if (!clients.contains(client)) {
            clients.add(client);
        }
    }
}
