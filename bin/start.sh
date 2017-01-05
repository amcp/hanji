#!/bin/bash
docker run -d --name es152kuromoji -p 9200:9200 -p 9300:9300 amcp/es152kuromoji
export ES_IP=`docker inspect --format '{{ .NetworkSettings.IPAddress }}' es152kuromoji`
#replace ES IP in tupl.properties TODO
sed -i.bak '/index.search.hostname/s/\(.*\)=.*$/\1='"$ES_IP"'/g' g/conf/tupl.properties
#start gremlin container
sleep 5s
docker run -d --name gremlin -v ${PWD}/g:/var/opt/graph -p 8182:8182 amcp/hanji
sleep 5s
#enable kuromoji
curl -XPOST 'http://0.0.0.0:9200/titan/_close'
curl -XPUT 'http://0.0.0.0:9200/titan/_settings' -d'
{
        "index":{
            "analysis":{
                "tokenizer" : {
                    "kuromoji_user_dict" : {
                       "type" : "kuromoji_tokenizer",
                       "mode" : "extended",
                       "discard_punctuation" : "false"
                    }
                },
                "analyzer" : {
                    "my_analyzer" : {
                        "type" : "custom",
                        "tokenizer" : "kuromoji_user_dict"
                    }
                }

            }
        }
}
'
curl -XPOST 'http://0.0.0.0:9200/titan/_open'
docker exec -i -t gremlin /bin/bash
