package eu.amcp.hanji

import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.Mapping
import com.thinkaurelius.titan.core.schema.TitanManagement
import com.thinkaurelius.titan.core.TitanGraph
import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * Created by amcp on 2017/01/07.
 */
class MetadataLoader {

    public static final String SEARCH = "search"
    public static final String BY_DETERMINING_LAWS = 'byDeterminingLaws'
    TitanGraph graph

    void add(record) {
        println record
    }

    static void main(args) {
        def storageDirectory = new File(args[0])
        def esServer = args[1]
        def esPort = args[2]
        try {
            new MetadataLoader(storageDirectory, esServer, esPort)
        } catch(Exception e) {
            e.printStackTrace()
            System.exit(1)
        }
        System.exit(0)
    }

    MetadataLoader(File dir, String esServer, esPort) {
        graph = configureOpenGraph(dir, esServer, esPort, 0/*mutations*/)
        def mgmt = graph.openManagement()
        mgmt.makeVertexLabel("ruling")
        def case_url = mgmt.makePropertyKey('caseUrl').dataType(String.class).make()
        def case_number = mgmt.makePropertyKey('case_number').dataType(String.class).make()
        def case_paradigm = mgmt.makePropertyKey('case_paradigm').dataType(String.class).make()
        def case_type = mgmt.makePropertyKey('case_type').dataType(String.class).make()
        def courthouse = mgmt.makePropertyKey('courthouse').dataType(String.class).make()
        def courthouse_section = mgmt.makePropertyKey('courthouse_section').dataType(String.class).make()
        def courtroom = mgmt.makePropertyKey('courtroom').dataType(String.class).make()
        def detail_url = mgmt.makePropertyKey('detail_url').dataType(String.class).make()
        def high_ruling_collection_volume_page = mgmt.makePropertyKey('high_ruling_collection_volume_page').dataType(String.class).make()
        def ruling_type = mgmt.makePropertyKey('ruling_type').dataType(String.class).make()
        def s3_annotated_url = mgmt.makePropertyKey('s3_annotated_url').dataType(String.class).make()
        def s3_text_url = mgmt.makePropertyKey('s3_text_url').dataType(String.class).make()
        def s3_url = mgmt.makePropertyKey('s3_url').dataType(String.class).make()
        def supreme_ruling_collection_volume_page = mgmt.makePropertyKey('supreme_ruling_collection_volume_page').dataType(String.class).make()

        //composite indexes - unique
        def hanji_id = mgmt.makePropertyKey('hanji_id').dataType(String.class).make()
        def category = mgmt.makePropertyKey('category').dataType(String.class).make()
        if (null == mgmt.getGraphIndex('byHanjiAndCategoryUnique')) {
            mgmt.buildIndex('byHanjiAndCategoryUnique', Vertex.class).addKey(hanji_id).addKey(category).unique().buildCompositeIndex()
        }

        //composite indexes
        createSingleVertexCompositeIndex(mgmt, 'hanji_id', 'byHanji', String.class)
        createSingleVertexCompositeIndex(mgmt, 'category', 'byCategory', String.class)
        createSingleVertexCompositeIndex(mgmt, 'parent_case_number', 'byParentCaseNumber', String.class)
        createSingleVertexCompositeIndex(mgmt, 'parent_jurisdiction', 'byParentJurisdiction', String.class)
        createSingleVertexCompositeIndex(mgmt, 'parent_ruling', 'byParentRuling', String.class)
        createSingleVertexCompositeIndex(mgmt, 'ruling', 'byRuling', String.class)
        createSingleVertexCompositeIndex(mgmt, 'ruling_date', 'byRulingDate', Long.class)

        //full text indexes
//        createSingleVertexFullTextIndex(mgmt, 'determining_laws', BY_DETERMINING_LAWS, String.class)
//        createSingleVertexFullTextIndex(mgmt, 'case_name', "byCaseName", String.class)
//        createSingleVertexFullTextIndex(mgmt, 'case_claim_summary', "byCaseClaimSummary", String.class)
//        createSingleVertexFullTextIndex(mgmt, 'case_claims', "byCaseClaims", String.class)
//        createSingleVertexFullTextIndex(mgmt, 'case_rights', "byCaseRights", String.class)
//        createSingleVertexFullTextIndex(mgmt, 'ruling_summary', "byRulingSummary", String.class)
//        createSingleVertexFullTextIndex(mgmt, 'ruling_text', "byRulingText", String.class)

        mgmt.commit()
    }

    private void createSingleVertexCompositeIndex(TitanManagement mgmt, String keyName, String indexName, Class<?> clazz) {
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

    private void createSingleVertexFullTextIndex(TitanManagement mgmt, String keyName, String indexName, Class<?> clazz) {
        def property
        if(false == mgmt.containsPropertyKey(keyName)) {
            property = mgmt.makePropertyKey(keyName).dataType(clazz).make()
        } else {
            property = mgmt.getPropertyKey(keyName)
        }
        if (null == mgmt.getGraphIndex(indexName)) {
            mgmt.buildIndex(indexName, Vertex.class).addKey(property, Mapping.TEXT.asParameter()).buildMixedIndex(SEARCH)
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
