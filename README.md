# Distributed Hash Table (P2P File Sharing System) — CS382 Network Centric Computing (Spring 2024)

This repository contains my implementation of a **peer-to-peer file sharing system** based on the **Distributed Hash Table (DHT)** model for the **CS382: Network Centric Computing** course at **LUMS**, taught by **Dr. Zartash Afzal Uzmi**.

This project implements a fault-tolerant, decentralized file-sharing system that uses **consistent hashing** for scalable node lookup and key distribution. The implementation focuses on the design of distributed systems with **resilience to node failure**, **efficient file transfers**, and **socket-based inter-node communication**.

---

## Implemented Features
This system implements the following functionality as described in the assignment:
- **Initialization:** Node setup with successor and predecessor tracking using hashed identifiers.  
- **Join:** Seamless node addition with successor/predecessor updates and consistent hashing placement.  
- **Put / Get:** Distributed file storage and retrieval based on hashed filenames and responsible nodes.  
- **File Transfer on Join:** Redistribution of files when a new node joins the network.  
- **Leave:** Graceful node departure with proper handoff of files to new responsible nodes.  
- **Failure Tolerance:** Successor list maintenance and replication for recovery in case of node failures.  
