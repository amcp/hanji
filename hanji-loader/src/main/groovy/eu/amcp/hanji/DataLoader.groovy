package eu.amcp.hanji

import com.amazonaws.services.dynamodbv2.document.Item
import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.TitanGraph
import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.structure.Vertex

import java.text.DateFormat
import java.text.SimpleDateFormat

/**
 * Created by amcp on 2017/01/11.
 */
class DataLoader {

    TitanGraph graph
    File rulingTextDir

    static void main(String[] args) {
        File data = new File(args[0])
        File storageDirectory = new File(args[1])
        String esServer = args[2]
        String esPort = args[3]
        File rulingTextDir = new File(args[4])
        DataLoader dl = null
        try {
            dl = new DataLoader(storageDirectory, esServer, esPort, rulingTextDir)
            Item dict = Item.fromJSON(data.text)
            for(int i = 1; i <= dict.numberOfAttributes(); i++) {
                dl.add(dict.getRawMap(i.toString()))
                dl.graph.tx().commit()
                if(i % 1000 == 0) {
                    print "commit at " + i + "\n"
                }
            }
            System.exit(0)
        } catch(Exception e) {
            e.printStackTrace()
            System.exit(1)
        } finally {
            dl.graph.close()
        }
    }

    DataLoader(File dir, String esServer, String esPort, File rulingTextDir) {
        this.rulingTextDir = rulingTextDir
        graph = configureOpenGraph(dir, esServer, esPort, 0/*mutations*/)
    }

    void add(Map<String, Object> item) {
        String hanji = item.get('hanji_id')
        Vertex v = graph.traversal().V().has('ruling', 'hanji_id', hanji)
                .has('category', item.get('category')).tryNext().orElse(graph.addVertex('ruling'))
        for (String attribute : item.keySet()) {
            def value = item.get(attribute)
            if (value != null) {
                if(attribute.equals("ruling_date")) {
                    DateFormat df = new SimpleDateFormat("GGGGy年M月d日", new Locale("ja", "JP", "JP"))
                    v.property(attribute, df.parse(value).toInstant().epochSecond)
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

    static TitanGraph configureOpenGraph(File storageDirectory, String esServer, String esPort, int mutations) {
        final BaseConfiguration conf = new BaseConfiguration()
        conf.setProperty("storage.batch-loading", "false") //needs to be false for autoschema
        conf.setProperty("storage.transactional", "false")
        conf.setProperty("storage.buffer-size", "1000000")
        conf.setProperty("storage.setup-wait", "5000")
        conf.setProperty("ids.block-size", 1000)
        //conf.setProperty("ids.flush", "false")

        //storage backend specific
        //TODO externalize this to a properties file
        conf.setProperty("storage.backend", "jp.classmethod.titan.diskstorage.tupl.TuplStoreManager")
        conf.setProperty("storage.directory", storageDirectory.getAbsolutePath())
        conf.setProperty("schema.default", "default")
        conf.setProperty("storage.tupl.prefix", "hanji")
        conf.setProperty("storage.tupl.min-cache-size", "100000000") //TODO should this be a function of something?
        conf.setProperty("storage.tupl.map-data-files", "true")
        conf.setProperty("storage.tupl.direct-page-access", "false") //requires JNA which seems broken?
//        conf.setProperty("index.search.backend", "elasticsearch")
//        conf.setProperty("index.search.elasticsearch.interface", "TRANSPORT_CLIENT")
//        conf.setProperty("index.search.hostname", esServer)
//        conf.setProperty("index.search.port", esPort)
//        conf.setProperty("index.search.map-name", "true")
//        conf.setProperty("index.search.cluster-name", "elasticsearch")
//        conf.setProperty("index.search.elasticsearch.health-request-timeout", "10s")
        final TitanGraph g = TitanFactory.open(conf)
        return g
    }
}
