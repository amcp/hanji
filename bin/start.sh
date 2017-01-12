#!/bin/bash
#export ES_IP=`docker network inspect bridge | grep Gateway | sed 's/.*:\ \"\(.*\)\"/\1/'`
#replace ES IP in tupl.properties
#sed -i.bak '/index.search.hostname/s/\(.*\)=.*$/\1='"$ES_IP"'/g' g/conf/tupl.properties
#start gremlin container
docker run -d --name gremlin -v ${PWD}/g:/var/opt/graph -p 8182:8182 amcp/hanji
while ! echo "not a websocket handshake" | nc 0.0.0.0 8182
do
    sleep 1s
done
docker exec -i -t gremlin /bin/bash
