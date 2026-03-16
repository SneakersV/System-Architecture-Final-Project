package server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * The main server class that starts the rmiregistry programmatically
 * and binds the Directory object.
 */
public class DirectoryServer {

    public static void main(String[] args) {
        try {
            int port = 1099;

            // Start the RMI registry locally
            // This is the pattern taught in the course: "running the rmiregistry within the
            // server JVM"
            Registry registry = LocateRegistry.createRegistry(port);
            System.out.println("RMI registry started on port " + port);

            // Create an instance of the server object
            DirectoryImpl directoryObj = new DirectoryImpl();

            // Bind the object to the naming service
            registry.rebind("Directory", directoryObj);

            System.out.println("Directory Server is ready and waiting for connections.");

        } catch (Exception e) {
            System.err.println("Directory Server exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
