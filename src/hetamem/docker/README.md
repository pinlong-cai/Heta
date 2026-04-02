## Docker Environment Configuration

The default versions of various components used in the Docker environment, installed via `docker-compose.yml`.

### Default Versions

- **Docker**: 4.35.0  
- **Milvus**: 2.4.0  
- **etcd**: 3.5.5  
- **MinIO**: RELEASE.2024-09-13T20-26-02Z  
- **Attu**: 2.4.0  
- **Neo4j**: 5.26.0


## 📌 Quick start
Create volume directories under the `docker` directory：
```bash
cd docker
mkdir -p milvus
mkdir -p neo4j
```

```bash
docker-compose up -d
```
This command will download and start the following containers:

- milvus: For vector similarity search
- minio: For object storage
- etcd: For distributed key-value storage

All images will be downloaded to Docker's default image storage location (/var/lib/docker/). Total size ~2GB, may take 5-10 minutes depending on your network speed.

If the following words are displayed, it indicates that the download is complete.
```
[+] Running 6/6
⠿ Network docker_milvus-network   Created
⠿ Container milvus-minio          Started
⠿ Container milvus-etcd           Started
⠿ Container neo4j-container       Started
⠿ Container milvus-standalone     Started
⠿ Container attu                  Started

```
## About Ports

Here is a list of the ports used by each service:

| Service       | Front-end Port | Read/Write Port |
|---------------|----------------|-----------------|
| Milvus        | 9091           | 19530           |
| Neo4j         | 7474           | 7687            |
| MinIO         | 9001           | 9000            |


