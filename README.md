# chord-file-sharing
## Overview
Chord File Sharing is a decentralized and peer-to-peer file-sharing application that utilizes the Chord DHT (Distributed Hash Table) protocol for efficient and scalable file distribution. This version of the project incorporates gRPC (gRPC Remote Procedure Calls) for communication between nodes, enhancing the efficiency and maintainability of the system.

## Table of Contents
- [Features](#features) 
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Usage](#usage)
- [Future Work](#future-work)
- [License](#license)

## Features
- Decentralized File Sharing: Chord File Sharing eliminates the need for a centralized server, allowing users to share files directly with each other.
- Scalability: The Chord DHT protocol enables scalable and efficient file sharing, making it suitable for a large number of users.
- Secure: The decentralized nature of the system enhances security, as there is no single point of failure or vulnerability.
- Persistent Content Hosting: Users can upload their content to the Chord ring, and it is stored on another node. This ensures that the content remains available even if the original owner leaves. Users can choose to host other content when they are online, contributing to the resilience and availability of shared files.
- Stabilization: The Chord ring is self-stabilizing, keeping nodes' successor pointers up to date. which is sufficient to guarantee correctness of lookups.

## Getting Started
To run the chord file sharing application, set two environment variables: `HOST` and `PORT`. 

- `PORT` is the port number that the node will listen on.
- `HOST` is the host address of the node. The host address can be a domain name or an IP address.

It will automatically create a folder named with the port number to store the files under `resources/<port>`. When app starts, it starts grpc-java as daemon thread by default.  

Example of setting environment variables in IntelliJ IDEA:
```bash
HOST=127.0.0.1;PORT=8000
```

## Usage
The first node starts the ring should call `join` with no arguments first to create a new ring. The rest of the nodes should call `join` with any known nodes host and port to join the ring.
### Commands

## Future Work
- [ ] Implement a GUI for the application.
- [ ] Support a ssl connection for grpc communication.
- [ ] Support when a node leaves the ring, the successor of the node will take over the files that the node is hosting.
- [ ] Implement a successor list to keep track of *r* nearest successors. This enables the node to find its correct successor.
- [ ] the file should store in multiple nodes to avoid single point of failure.

## License
[license](https://github.com/FengyiQuan/chord-file-sharing/blob/main/LICENSE)