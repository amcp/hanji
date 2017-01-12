#!/bin/bash
#run data downloader
#download es
#enable dynamic groovy scripts - script.disable_dynamic: false
#http://stackoverflow.com/questions/24711168/elasticsearch-dynamic-scripting-disabled
# clean everything
rm -rf g/data/*db elasticsearch-1.5.1/data/elasticsearch
#start es
elasticsearch-1.5.1/bin/elasticsearch 1>${PWD}/elasticsearch-1.5.1/service.log 2>${PWD}/elasticsearch-1.5.1/service.err &
while ! curl http://0.0.0.0:9200
do
    echo "$(date) - still waiting for es to start"
    sleep 1s
done
ES_PID=`ps x | grep elastic | grep java | cut -f2 -d\ `
#create schema
pushd hanji-loader
./gradlew schema
popd
#enable kuromori
bin/enableKuromoji.sh
pushd hanji-loader
./gradlew loadData
