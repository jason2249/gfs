# Simplified GFS
Simplified GFS is, as its name implies, a simplified version of the [Google File System](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf). It is written in Python and attempts to emulate the overall architecture and design of the original Google File System while also maintaining a level of simplicity for both client use and programmer development. It allows a client to create, read, write, and delete files within a distributed filesystem while also allowing for recovery from both master and chunkserver failures.

Usage:

Run `python3 master.py` to run the master server, of which there should be only 1.

Run `python3 chunkserver.py <host> <port>` to run a chunkserver. The number of chunkservers should be greater than or equal to the number of replicas required for a chunk.
