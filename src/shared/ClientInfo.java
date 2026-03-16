package shared;

import java.io.Serializable;

/**
 * Stores information about a client (Daemon) that holds a specific file.
 * Must implement Serializable to be sent over RMI.
 */
public class ClientInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String ip;
    private int port;

    public ClientInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientInfo that = (ClientInfo) o;
        return port == that.port && ip.equals(that.ip);
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
