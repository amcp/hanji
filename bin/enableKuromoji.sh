#!/bin/bash
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