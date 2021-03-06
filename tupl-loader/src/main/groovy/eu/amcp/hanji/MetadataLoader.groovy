package eu.amcp.hanji

import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.schema.JanusGraphManagement

/**
 * Created by amcp on 2017/01/07.
 */
class MetadataLoader {

    public static final String RULING = "ruling"
    Graph graph

    static void main(args) {
        def storageDirectory = new File(args[0])
        try {
            new MetadataLoader(storageDirectory)
        } catch (Exception e) {
            e.printStackTrace()
            System.exit(1)
        }
        System.exit(0)
    }

    MetadataLoader(File dir) {
        def stringProps = ['caseUrl', 'case_number', 'case_paradigm', 'case_type', 'courthouse',
                           'courthouse_section', 'courtroom', 'detail_url',
                           'high_ruling_collection_volume_page', 'ruling_type', 's3_annotated_url',
                           's3_text_url', 's3_url', 'supreme_ruling_collection_volume_page',
                           'determining_laws', 'case_name', 'case_claim_summary', 'case_claims',
                           'case_rights', 'ruling_summary', 'ruling_text']
        def attrIndexMap = [
                hanji_id: ['byHanji', String.class],
                category: ['byCategory', String.class],
                parent_case_number: ['byParentCaseNumber', String.class],
                parent_jurisdiction: ['byParentJurisdiction', String.class],
                parent_ruling: ['byParentRuling', String.class],
                ruling: ['byRuling', String.class],
                ruling_date: ['byRulingDate', String.class]
        ]

        def titanGraph = openTitanGraph(dir, 100000000/*mutations*/)
        graph = titanGraph
        def management = titanGraph.openManagement()
        management.makeVertexLabel(RULING)
        management.makeVertexLabel("opinion")
        stringProps.each {
            makeProperty(management, it, String.class)
        }
        //composite indexes - unique
        def hanji_id_category = makeProperty(management, 'hanji_id_category', String.class)
        if (null == management.getGraphIndex('byHanjiAndCategoryUnique')) {
            management.buildIndex('byHanjiAndCategoryUnique', Vertex.class).addKey(hanji_id_category).unique().buildCompositeIndex()
        }

        //remaining composite indexes
        attrIndexMap.each { key, value ->
            createSingleVertexCompositeIndex(management, key, value[0], value[1])
        }

        management.commit()

    }

    private PropertyKey makeProperty(JanusGraphManagement mgmt, String name, Class<?> clazz) {
         return mgmt.makePropertyKey(name).dataType(clazz).make()
    }

    private void createSingleVertexCompositeIndex(JanusGraphManagement mgmt, String keyName, String indexName, Class<?> clazz) {
        def property
        if(false == mgmt.containsPropertyKey(keyName)) {
            property = mgmt.makePropertyKey(keyName).dataType(clazz).make()
        } else {
            property = mgmt.getPropertyKey(keyName)
        }
        if (null == mgmt.getGraphIndex(indexName)) {
            mgmt.buildIndex(indexName, Vertex.class).addKey(property).buildCompositeIndex()
        }
    }



    static JanusGraph openTitanGraph(File storageDirectory, int mutations) {
        final BaseConfiguration conf = new BaseConfiguration()
        conf.setProperty("storage.batch-loading", "false") //needs to be false for autoschema
        conf.setProperty("storage.transactional", "false")
        conf.setProperty("storage.buffer-size", mutations.toString())
        conf.setProperty("storage.setup-wait", "5000")
        conf.setProperty("ids.block-size", 1000)
        //conf.setProperty("ids.flush", "false")

        //storage backend specific
        //TODO externalize this to a properties file
        conf.setProperty("storage.backend", "berkeleyje")
        conf.setProperty("storage.directory", storageDirectory.getAbsolutePath())
        conf.setProperty("schema.default", "default")
//        conf.setProperty("storage.tupl.prefix", "hanji")
//        conf.setProperty("storage.tupl.min-cache-size", "100000000") //TODO should this be a function of something?
//        conf.setProperty("storage.tupl.map-data-files", "true")
//        conf.setProperty("storage.tupl.direct-page-access", "false") //requires JNA which seems broken?
        final JanusGraph g = JanusGraphFactory.open(conf)
        return g
    }
}
