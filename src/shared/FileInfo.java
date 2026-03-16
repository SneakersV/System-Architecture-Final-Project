package shared;

import java.io.Serializable;
import java.util.ArrayList;
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
    private List<ClientInfo> clients;

    public FileInfo(String filename, long fileSize) {
        this.filename = filename;
        this.fileSize = fileSize;
        this.clients = new ArrayList<>();
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
        // Prevent duplicate clients
        for (ClientInfo c : clients) {
            if (c.getIp().equals(client.getIp()) && c.getPort() == client.getPort()) {
                return;
            }
        }
        this.clients.add(client);
    }
}
