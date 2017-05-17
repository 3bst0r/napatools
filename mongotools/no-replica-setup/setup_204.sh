# install
bash scripts/install-3.2.sh

# os config
ulimit -n 24000

# start mongos
mongos --config cfg/mongos_server.cfg

# add nodes to cluster
mongo --port 27021 < js/add-shards-to-cluster.js

# set cursor timeout
mongo --port 27021 < js/set-cursor-timeout.js

# setup ycsb database
mongo --port 27021 < js/setup-ycsb-database.js
