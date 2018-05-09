set -x

# os config
ulimit -n 10270

# start mongod replica set
numactl --interleave=all mongod --config cfg/shard_192.cfg

# set mongod replicas
mongo  --port 27018 < js/setup-data-replicas-for-192.js

# set cursor timeout
mongo --port 27018 < js/set-cursor-timeout.js