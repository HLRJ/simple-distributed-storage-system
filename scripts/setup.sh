rm -rf data

../bin/namenode -addr localhost:8000 -replicaid 1 &
../bin/namenode -addr localhost:8001 -replicaid 2 &
../bin/namenode -addr localhost:8002 -replicaid 3 &

sleep 5

../bin/datanode -addr localhost:9000 &
../bin/datanode -addr localhost:9001 &
../bin/datanode -addr localhost:9002 &