FROM amcp/tupl-titan-server
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python get-pip.py
RUN pip install awscli
RUN sed -i.bak 's/Xmx512m/Xmx10g/' /gremlin/bin/gremlin-server.sh
EXPOSE 8182
ENTRYPOINT /gremlin/bin/gremlin-server.sh /var/opt/graph/conf/server.yaml 1>/var/opt/graph/log/service.log 2>/var/opt/graph/log/service.err
