# Parallel Download Infrastructure - Project Report
**Authors:** [Chu Hoang Viet / Bui Dang Quang]

## 1. System Prototype & Architecture
This project implements a distributed system for parallel file downloading to improve overall speed by fetching fragments from multiple sources simultaneously. 

### 1.1 Core Components
- **Directory Server (RMI):** Acts as a centralized registry. It tracks which files are available and which node addresses are currently hosting them.
- **Daemon (TCP Server & RMI Client):** A background service running on each node. Upon startup, it informs (1) the Directory about the available files in a specific folder. It then listens for incoming TCP requests to serve file fragments.
- **Download (TCP Client & RMI Client):** When a download starts, this component requests (2) the Directory to get the list of clients where the file is available, and then downloads (3) different fragments of the file in parallel from different clients.

### 1.2 Parallelism Logic
The system optimizes file transfers by dividing a single file into $N$ equal fragments (where $N$ is the number of available sources). Each fragment is assigned to a dedicated `FragmentDownloader` thread, which establishes a direct TCP socket connection to a remote Daemon. This allows for high-throughput data transfer by utilizing the concurrent upload bandwidth of multiple nodes.

## 2. Enhancements & Technical Robustness
Beyond the basic prototype, the following technical optimizations were implemented:

### 2.1 Failure and Disconnection Handling
The system demonstrates high resilience during active transfers. If a client serving a fragment fails or disconnects midway, the `Download` component automatically catches the socket exception. It then identifies the remaining bytes and seamlessly migrates the request to another available healthy source to resume the download without user intervention.

### 2.2 Dynamic Adaptation
The Directory Server maintains a real-time view of the network:
- **Client Integration**: New clients are integrated automatically as their Daemon services register files.
- **Auto-Cleanup**: A heartbeat mechanism detects disconnected clients. If a node fails to send a heartbeat within 8 seconds, the Directory purges its entries to ensure future downloads are only directed to live sources.
- **Source Vetting**: Before initiating a parallel download, the client performs an active TCP "liveness" probe on all reported sources to ensure they are reachable.

### 2.3 Optimization & Cross-Platform Stability
- **Source Selection (Load Monitoring)**: The system tracks the transmission rates of each client. If a failure occurs, it prioritizes the machine with the highest historical throughput for the recovery process.
- **Windows Parallel I/O**: We utilized `java.nio.channels.FileChannel` to solve disk-locking bottlenecks common on Windows. This enables true positional parallel writes, allowing multiple threads to write to the same file at different offsets without interference.
- **Self-Source Filtering**: To prevent redundant network loops and potential metadata conflicts, the node automatically filters out its own IP from the list of available sources during a download.

## 3. How to Run the System

### 3.1 Setup
1. **Directory Structure:**
   - `src/`: Java source code.
   - `shared_file/`: Folder for shared files (place test files here).
2. **Compile:**
   ```powershell
   # Clean bin folder
   rm -r -fo bin ; mkdir bin
   # Compile all modules
   javac -d bin src/shared/*.java src/server/*.java src/client/*.java
   ```

### 3.2 Scenario A: Single Machine (Localhost - 127.0.0.1)
1. **Start the Directory Server:**
   ```powershell
   java -cp bin server.DirectoryServer
   ```
2. **Start Integrated P2P Client Nodes:**
   *(Open separate terminals for each node)*
   ```powershell
   # Terminal for Node 1
   java -cp bin client.ClientNode 127.0.0.1 shared_file
   # Terminal for Node 2
   java -cp bin client.ClientNode 127.0.0.1 shared_file
   ```

### 3.3 Scenario B: Multiple Machines (Tailscale / LAN)
To ensure connectivity across different networks, use a VPN/Overlay like Tailscale and configure the RMI hostname.

**1. Start the Directory Server:**
```powershell
# Set hostname to THIS machine's Tailscale IP (starts with 100.x.x.x)
java "-Djava.rmi.server.hostname=<SERVER_IP>" -cp bin server.DirectoryServer
```

**2. Start P2P Client Nodes:**
```powershell
# Set hostname to THIS machine's IP, and point to the Directory Server IP
java "-Djava.rmi.server.hostname=<CLIENT_IP>" -cp bin client.ClientNode <SERVER_IP> shared_file
```

### 3.4 Usage & Commands
Once the node starts, the following commands are available:
- `download <filename>`: Initiates the parallel fetching process.
- `exit`: Safely unregisters and stops the node.

### 3.5 Troubleshooting
- **ClassNotFoundException:** Ensure you are running commands from the root directory and the `bin` folder is correctly populated.
- **Vetting Error:** If the system says "No valid remote sources available", confirm that other `ClientNode` instances are running and that their Daemons have successfully registered their files.

---
**Technical Note:** This system is optimized for high-concurrency P2P transfers using Java's NIO and RMI frameworks.
