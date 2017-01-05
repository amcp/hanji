FROM elasticsearch:1.5.2
RUN plugin install elasticsearch/elasticsearch-analysis-kuromoji/2.5.0
EXPOSE 9200/tcp 9300/tcp
HEALTHCHECK CMD curl 'localhost:9200/_cat/health?v' 2>&1 | grep green > /dev/null
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["elasticsearch"]
