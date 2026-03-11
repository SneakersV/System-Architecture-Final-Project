import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface DirectoryInterface extends Remote {
    /**
     * Called by a Daemon to register its available files.
     * @param clientInfo the client's ip and port
     * @param files the list of files owned by the client
     */
    void registerClient(ClientInfo clientInfo, List<String> files) throws RemoteException;
    
    /**
     * Called by a Daemon when it shuts down gracefully.
     * @param clientInfo the client's ip and port
     */
    void unregisterClient(ClientInfo clientInfo) throws RemoteException;

    /**
     * Called by a Download client to find who has a file.
     * @param filename the name of the file
     * @return a list of clients possessing the file
     */
    List<ClientInfo> lookupFile(String filename) throws RemoteException;
}
