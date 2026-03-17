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
   rm -r -fo bin ; mkdir bin
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
2. **Start Integrated P2P Client Nodes:**
   *(Open separate terminals for each node)*
   ```bash
   # Terminal for Node 1
   java -cp bin client.ClientNode 127.0.0.1 <folder_path>

   # Terminal for Node 2
   java -cp bin client.ClientNode 127.0.0.1 <folder_path>
   ```
3. **Usage inside ClientNode CLI:**
   Once the node starts, you can type commands:
   - `download <filename>`: Example: `download large_file.dat`
   - `exit`: To stop the node.

### 3.3 Scenario B: Running on Multiple Machines (LAN or Different Wi-Fi)
If testing across different Wi-Fi networks, use **Tailscale** to bridge the computers.

1.  **Preparation (on all machines):**
    - Install **Tailscale** and log in with the *same* account.
    - Identify each machine's Tailscale IP (starts with `100.x.x.x`).
    - *Example:* **Machine 1** (Server Host) = `100.64.0.1`, **Machine 2** (Client Node) = `100.64.0.2`.

2.  **On Machine 1 (Running the Directory Server):**
    ```powershell
    # -D flag MUST be in quotes for PowerShell. Set it to THIS machine's IP.
    java "-Djava.rmi.server.hostname=100.64.0.1" -cp bin server.DirectoryServer
    ```

3.  **On Machine 2 (Integrated P2P Node):**
    ```powershell
    # 1. Set hostname to THIS machine's IP (Machine 2)
    # 2. Last argument is the IP of the Directory Server (Machine 1)
    java "-Djava.rmi.server.hostname=100.64.0.2" -cp bin client.ClientNode 100.64.0.1 <folder_path>
    ```

4.  **Communication:**
    In Machine 2's terminal, type `download <filename>` to fetch files from Machine 1.
