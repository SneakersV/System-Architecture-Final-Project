package shared;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The remote interface for the Directory server.
 */
public interface Directory extends Remote {

    /**
     * Registers a file hosted by a specific client.
     * 
     * @param filename The name of the file
     * @param fileSize The size of the file in bytes
     * @param client   The client holding the file
     * @throws RemoteException
     */
    void registerFile(String filename, long fileSize, ClientInfo client) throws RemoteException;

    /**
     * Looks up a file to get its information and the list of clients holding it.
     * 
     * @param filename The name of the file
     * @return FileInfo containing size and list of clients, or null if not found
     * @throws RemoteException
     */
    FileInfo lookupFile(String filename) throws RemoteException;

    /**
     * Signals that a client is still active.
     * 
     * @param client The client info
     * @throws RemoteException
     */
    void heartBeat(ClientInfo client) throws RemoteException;

    /**
     * Explicitly unregisters a client from all files it was hosting.
     * 
     * @param client The client info
     * @throws RemoteException
     */
    void unregisterClient(ClientInfo client) throws RemoteException;
}
