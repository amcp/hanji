#!/bin/bash
docker run -d --name es152kuromoji -p 9200:9200 -p 9300:9300 amcp/es152kuromoji
while ! curl http://0.0.0.0:9200
do
  echo "$(date) - still trying"
  sleep 1s
done

