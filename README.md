# Parallel Download Infrastructure - Project Report
**Authors:** [Your Name / Team Name]

## 1. Project Overview & Architecture
This project implements a distributed system for parallel file downloading to improve overall speed by fetching fragments from multiple sources simultaneously. The architecture comprises three main components:
- **Directory Server (RMI):** Acts as a centralized registry. It tracks which files are available and which `Daemon` nodes are currently hosting them.
- **Daemon (TCP Server & RMI Client):** Runs on host machines. Upon startup, it registers its shared local files with the Directory Server. It then listens for incoming TCP connections to serve specific file fragments to `Download` clients.
- **Download (TCP Client & RMI Client):** The user-facing client. It queries the Directory Server for file locations, divides the file into chunks, and spawns multiple parallel threads to fetch fragments from different `Daemons` simultaneously.

## 2. Implemented Features & Enhancements

### 2.1 Basic Prototype Capabilities (Mandatory)
- **Parallel File Downloading:** The requested file is split into independent sub-tasks based on the number of available sources.
- **Concurrent TCP Connections:** The client opens parallel threads to fetch chunks directly from the corresponding Daemons.
- **Automatic File Registration:** Daemons automatically scan a target directory and report their files to the Server.
- **Dynamic File Discovery:** A background scanner in the Daemon picks up newly added files every 5 seconds without needing a restart.
- **Failure Recovery:** If a `Daemon` disconnects midway, the `Download` client can catch the exception and redirect the missing fragment request to the surviving Daemons, without dropping the entire download.

### 2.2 Enhancements (Extra Features)
**1. Dynamic Adaptation (Client Disconnection & Heartbeat)**
To ensure the Directory remains accurate when nodes fail or leave ungracefully, we implemented a periodic Heartbeat mechanism in the `Daemon`. 
- The `Daemon` sends a heartbeat to the Directory every 5 seconds.
- A background `Timer` thread in the Directory checks for stale clients (no heartbeat for 15s) and automatically removes them from the file registry, ensuring new downloads don't connect to dead nodes.
- A JVM Shutdown Hook was also added to the `Daemon` to proactively send an `unregister` signal upon normal exit (Ctrl+C).

**2. Source Selection Optimization (Performance Tracking)**
The `Download` client was upgraded to track the download speed (bytes/ms) of each source dynamically based on past fragments completed.
- When an active fragment download fails due to a network error, the client must resume the fragment from another source.
- Instead of blindly picking the next source in a basic round-robin fashion, the client now selects the alternative source that has demonstrated the highest transfer rate so far during the session.

## 3. How to Run the System

### 3.1 Initial Setup
1. **Directory Structure:** Ensure your project folder matches the following structure (create missing directories as needed):
   - `src/`: Contains the Java source code (`client/`, `server/`, `shared/`).
   - `shared_data/`: Root folder for files you want to share.
     - `shared_data/client1/`: Folder for the first source. Place a test file (e.g., `large_file.dat`) here.
     - `shared_data/client2/`: Folder for the second source. Place the *same* test file here.
   - `downloads/`: An empty folder where the downloaded file will be saved.

2. **Clean up and Compile:**
   ```bash
   # Windows
   rmdir /s /q bin & mkdir bin
   javac -d bin src/shared/*.java src/server/*.java src/client/*.java

   # Linux/Mac
   rm -rf bin && mkdir bin
   javac -d bin src/shared/*.java src/server/*.java src/client/*.java
   ```

### 3.2 Scenario A: Running on a single computer (Localhost - 127.0.0.1)
1. **Start the Directory Server:**
   ```bash
   java -cp bin server.DirectoryServer
   ```
2. **Start the Daemons (Sources):**
   *(Open separate terminals for each source)*
   ```bash
   # Terminal for Source 1 (port will be assigned automatically)
   java -cp bin client.Daemon 127.0.0.1 ./shared_data/client1

   # Terminal for Source 2
   java -cp bin client.Daemon 127.0.0.1 ./shared_data/client2
   ```
3. **Start the Download Client:**
   ```bash
   # Terminal for Download Client. (Download file into folder downloads)
   java -cp bin client.Download <filename> 127.0.0.1 ./downloads
   ```

### 3.3 Scenario B: Running on Multiple Machines (LAN/Internet)
This system requires Java RMI and TCP Sockets. When running on separate machines, you must explicitly set the `java.rmi.server.hostname` property.

1. **Identify IP Addresses:**
   - Use `ipconfig` (Windows) or `ip a` (Linux/Mac) to find the IPv4 address of each machine.
   - *Example: Server IP = `192.168.1.5`, Daemon A IP = `192.168.1.10`.*

2. **On Machine 1 (Directory Server):**
   ```bash
   java -Djava.rmi.server.hostname=192.168.1.5 -cp bin server.DirectoryServer
   ```
3. **On Machine 2 (Daemon Server):**
   ```bash
   java -Djava.rmi.server.hostname=192.168.1.10 -cp bin client.Daemon 192.168.1.5 ./src/shared_folder
   ```
4. **On Machine 3 (Download Client):**
   ```bash
   java -cp bin client.Download <filename> 192.168.1.5 ./downloads
   ```
