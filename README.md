# Violin

A distributed key-value store written in Go. Purely an experiment and learning exercise.

### Usage

#### Start a server

Start a Violin server with the `server` command:

```sh
violin server --bootstrap
```

Arguments:

* `--bind`: Host and port to which the server should bind. Defaults to 127.0.0.1:5000. The port given is used by Serf for service discovery.
* `--rpc-port`: Port to which the server should bind for RPC. Defaults to 5001.
* `--data-dir`: Path where server data will be stored.
* `--bootstrap`: If set, the server will attempt to bootstrap itself. This is only permitted the first time the server is run and will fail otherwise.
* `--start-addrs`: Comma-separated list of addresses for existing Violin nodes to which the server should attempt to connect.
* `--node-name`: An optional name for the node. When blank this defaults to the `--bind` value.

For example, to start a three-node cluster:

```sh
violin server \
    --bind 127.0.0.1:5000 \
    --rpc-port 5001 \
    --data-dir /tmp/violin1 \
    --bootstrap
    
violin server \
    --bind 127.0.0.1:5002 \
    --rpc-port 5003 \
    --data-dir /tmp/violin2 \
    --start-addrs 127.0.0.1:5000
    
violin server \
    --bind 127.0.0.1:5004 \
    --rpc-port 5005 \
    --data-dir /tmp/violin3 \
    --start-addrs 127.0.0.1:5000
```

The first server will start in Bootstrap mode and be automatically elected as leader. The second and third server will then join the cluster as voters. Serf wil ensure that all nodes are kept up-to-date with nodes joining or leaving the cluster, while Raft will replicate data between nodes.

Reads and write are accepted on any node in the cluster.

#### Client

The Violin client commands allow for setting string keys and values. All commands require a host and port to which the client should connect.

```sh
violin client 127.0.0.1:5000 set hello world
# => Value set

violin client 127.0.0.1:5000 get hello
# => hello = world
```
