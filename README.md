<br>

<p>
  <img src="./logo.png" />
</p>
<br>

A distributed, fault-tolerant key-value store. 

The data is persisted on the disk using [BadgerDB](https://github.com/dgraph-io/badger). Replication and fault-tolerance is achieved using [Raft](https://raft.github.io).

It also provides a simple HTTP API for accessing the data.

<br>

## Installation


### From Source
1. Clone the repository
```
git clone https://github.com/Mohamed-Eldakak/SESHAT.git
cd seshat
```
2. Build the binary
```
go mod download
go build ./cmd/seshat
sudo mv ./seshat /usr/local/bin/
```
This will build the `seshat` binary and place it in the `/usr/local/bin` directory. You can now run the binary using `seshat`.

<br>

## Usage

```
$ seshat --help
NAME:
   seshat - A simple fault tolerant key-value store

USAGE:
   seshat [global options] command [command options] [arguments...]

DESCRIPTION:
   A distributed fault-tolerant key-value store which uses Raft for consensus.

COMMANDS:
   run, r   Run the server
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h  show help (default: false)
```

<br>

## Running a node

```
$ seshat run --server-port 2221 --node-id n1 --raft-port 1111 --volume-dir /tmp/seshat/n1
```

<br>

## Settting up a cluster

* Run multiple nodes.
    ```
    $ seshat run --server-port 2221 --node-id n1 --raft-port 1111 --volume-dir /tmp/seshat/n1
    $ seshat run --server-port 2222 --node-id n2 --raft-port 1112 --volume-dir /tmp/seshat/n2
    $ seshat run --server-port 2223 --node-id n3 --raft-port 1113 --volume-dir /tmp/seshat/n3
    $ seshat run --server-port 2224 --node-id n4 --raft-port 1114 --volume-dir /tmp/seshat/n4
    ```
    Each node will be initialized as a leader. We'll have to connect the nodes to form a cluster.

* Connect the nodes to form a cluster.

    Manually pick one node as the leader and connect it to the rest of the nodes.
    
    For node `n2`
    ```
    $ curl --location --request POST 'localhost:2221/raft/join' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "node_id": "n2", 
        "raft_address": "127.0.0.1:1112"
    }'
    ```
    For node `n3`
    ```
    $ curl --location --request POST 'localhost:2221/raft/join' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "node_id": "n3", 
        "raft_address": "127.0.0.1:1113"
    }'
    ```
        For node `n4`
    ```
    $ curl --location --request POST 'localhost:2221/raft/join' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "node_id": "n4", 
        "raft_address": "127.0.0.1:1114"
    }'
    ```
    Then, check each of this endpoint, it will return the status that the port 2221 is now the only leader and the other is just a follower:
    * `http://localhost:2221/raft/stats`
    * `http://localhost:2222/raft/stats`
    * `http://localhost:2223/raft/stats`

    A 3 node cluster is now formed.

    <br>

## Reading and Writing Data
Once the cluster is formed, we can start sending HTTP requests to the leader node to read, write and delete key-value pairs.

We can read the data from any node in the cluster.

Each node exposes following endpoints:

* URL: `/store/:key`
    * Method: `GET`
    * Response: `200`
        ```json
        {
            "data": {
                "key":   "key",
                "value": "value"
		    },
            "message": "success fetching data"
        }
        ```
* URL: `/store/`
    * Method: `POST`
    * Request:
        ```json
        {
            "key":   "key",
            "value": "value" 
        }
        ```
    * Response: `200`
        ```json
        {
            "data": {
                "key":   "key",
                "value": "value" 
            },
            "message": "success fetching data"
        }
        ```

* URL: `/store/:key`
    * Method: `DELETE`
    * Response: `200`
        ```json
        {
            "data": {
                "key":  "key",
                "value": null
            },
            "message": "success removing data"
        }
        ```

## Removing a node

* URL: `/raft/remove`
    * Method: `POST`
    * Request:
        ```json
        {
            "node_id": "n1",
            "raft_address": "127.0.0.1:1111"
        }
        ```
    * Response: `200`
        ```
        {
            "data":{
                ...node stats,
            },
            "message":"node n1 removed successfully"
        }
        ```
