package eu.amcp.hanji

import com.amazonaws.services.dynamodbv2.document.Item
import com.google.common.base.Stopwatch
import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.TitanGraph
import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

/**
 * Created by amcp on 2017/01/11.
 */
class DataLoader {

    Graph graph
    File rulingTextDir
    Locale jp = new Locale("ja", "JP", "JP")
    Locale us = new Locale("us", "US")
    DateFormat jpFormat = new SimpleDateFormat("GGGGy年M月d日", jp)
    DateFormat usFormat = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSSZ", us)

    static void main(String[] args) {
        File data = new File(args[0])
        File storageDirectory = new File(args[1])
        File rulingTextDir = new File(args[2])
        String graphType = args[3]
        DataLoader dl = null
        try {
            dl = new DataLoader(storageDirectory, rulingTextDir, graphType)
            Item dict = Item.fromJSON(data.text)
            print "read data from disk\n"
            List<Map<String, Object>> rawItems = new ArrayList<>(dict.numberOfAttributes())
            int i
            for(i = 1; i <= dict.numberOfAttributes(); i++) {
                rawItems.add(dict.getRawMap(i.toString()))
            }
            Stopwatch timer = Stopwatch.createStarted()

            for(i = 0; i < rawItems.size(); i++) {
                dl.add(rawItems.get(i))
                if(i % 10000 == 0) {
                    print "at " + i + "\n"
                }
            }
            dl.graph.tx().commit()
            timer.stop()
            print "committing " + i + " cases on " + graphType + " took " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms"
        } catch(Exception e) {
            e.printStackTrace()
            System.exit(1)
        } finally {
            dl.graph.close()
        }
        System.exit(0)
    }

    DataLoader(File dir, File rulingTextDir, String graphType) {
        this.rulingTextDir = rulingTextDir
        if('neo4j'.equals(graphType)) {
            graph = Neo4jGraph.open(dir.getAbsolutePath())
        } else if("titan".equals(graphType)) {
            graph = openTitan(dir, 100000000/*mutations*/)
        } else {
            throw new IllegalArgumentException("graph type should be titan or neo4j")
        }
    }

    static Vertex createVertexWithCompositeProperty(Graph g, String hanji, String category) {
        Vertex added = g.addVertex('ruling')
        added.property("hanji_id_category", hanji + "/" + category)
        return added
    }

    void add(Map<String, Object> item) {
        String hanji = item.get('hanji_id')
        String category = item.get('category')
        Vertex v = graph.traversal().V().has('ruling', 'hanji_id', hanji)
                .has('category', category).tryNext().orElse(createVertexWithCompositeProperty(graph, hanji, category))

        for (String attribute : item.keySet()) {
            def value = item.get(attribute)
            if (value != null) {
                if(attribute.equals("ruling_date")) {
                    v.property(attribute, usFormat.format(jpFormat.parse(value)))
                } else {
                    v.property(attribute, value)
                }
            }
        }
        String base = hanji.substring(3, 6)
        File ruling = new File(rulingTextDir,  base + "/" + hanji + ".txt")
        if (ruling.exists()) {
            v.property("ruling_text", ruling.text)
        }
    }

    static TitanGraph openTitan(File storageDirectory, int mutations) {
        final BaseConfiguration conf = new BaseConfiguration()
        conf.setProperty("storage.batch-loading", "false") //needs to be false for autoschema
        conf.setProperty("storage.transactional", "false")
        conf.setProperty("storage.buffer-size", mutations.toString())
        conf.setProperty("storage.setup-wait", "5000")
        conf.setProperty("ids.block-size", 1000)
        //conf.setProperty("ids.flush", "false")

        //storage backend specific
        //TODO externalize this to a properties file
        conf.setProperty("storage.backend", "jp.classmethod.titan.diskstorage.tupl.TuplStoreManager")
        conf.setProperty("storage.directory", storageDirectory.getAbsolutePath())
        conf.setProperty("schema.default", "default")
        conf.setProperty("storage.tupl.prefix", "hanji")
        conf.setProperty("storage.tupl.min-cache-size", "3000000000") //TODO should this be a function of something?
        conf.setProperty("storage.tupl.map-data-files", "true")
        conf.setProperty("storage.tupl.direct-page-access", "false") //requires JNA which seems broken?
        final TitanGraph g = TitanFactory.open(conf)
        return g
    }
}
