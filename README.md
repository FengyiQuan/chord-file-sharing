# chord-file-sharing
## Overview
Chord File Sharing is a decentralized and peer-to-peer file-sharing application that utilizes the Chord DHT (Distributed Hash Table) protocol for efficient and scalable file distribution. This version of the project incorporates gRPC (gRPC Remote Procedure Calls) for communication between nodes, enhancing the efficiency and maintainability of the system.

## Table of Contents
- [Features](#features) 
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)

## Features
- Decentralized File Sharing: Chord File Sharing eliminates the need for a centralized server, allowing users to share files directly with each other.
- Scalability: The Chord DHT protocol enables scalable and efficient file sharing, making it suitable for a large number of users.
- Secure: The decentralized nature of the system enhances security, as there is no single point of failure or vulnerability.
- Persistent Content Hosting: Users can upload their content to the Chord ring, and it is stored on another node. This ensures that the content remains available even if the original owner leaves. Users can choose to host other content when they are online, contributing to the resilience and availability of shared files.

## Notes
- grpc-java uses daemon threads by default
- 
## License
[license](https://github.com/FengyiQuan/chord-file-sharing/blob/main/LICENSE)