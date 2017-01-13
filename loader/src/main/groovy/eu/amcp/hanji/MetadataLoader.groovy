package eu.amcp.hanji

import com.thinkaurelius.titan.core.PropertyKey
import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.TitanManagement
import com.thinkaurelius.titan.core.TitanGraph
import org.apache.commons.configuration.BaseConfiguration
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.schema.IndexDefinition
import org.neo4j.graphdb.schema.Schema
import org.neo4j.tinkerpop.api.impl.Neo4jGraphAPIImpl

import java.lang.reflect.Field

/**
 * Created by amcp on 2017/01/07.
 */
class MetadataLoader {

    public static final String RULING = "ruling"
    Graph graph

    static void main(args) {
        def storageDirectory = new File(args[0])
        def graphType = args[1]
        try {
            new MetadataLoader(storageDirectory, graphType)
        } catch(Exception e) {
            e.printStackTrace()
            System.exit(1)
        }
        System.exit(0)
    }

    IndexDefinition indexNeoProperty(Schema schema, String label, String property) {
        return schema.indexFor(DynamicLabel.label(label)).on(property).create()
    }

    MetadataLoader(File dir, graphType) {
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

        if('neo4j'.equals(graphType)) {
            def neo4j = Neo4jGraph.open(dir.getAbsolutePath())
            graph = neo4j

            def base = (Neo4jGraphAPIImpl) neo4j.getBaseGraph()
            Field f = base.getClass().getDeclaredField("db"); //NoSuchFieldException
            f.setAccessible(true);
            GraphDatabaseService gdb = (GraphDatabaseService) f.get(base); //IllegalAccessException
            def tx = base.tx()
            try {
                def schema = gdb.schema()
                schema.constraintFor(DynamicLabel.label(RULING)).assertPropertyIsUnique("hanji_id_category").create()
                attrIndexMap.each { key, value ->
                    indexNeoProperty(schema, RULING, key)
                }

                tx.success()
            } catch(Exception e) {
                tx.failure()
            }
            tx.close()
        } else if ("titan".equals(graphType)) {
            def titanGraph = openTitanGraph(dir, 100000000/*mutations*/)
            graph = titanGraph
            def management = titanGraph.openManagement()
            management.makeVertexLabel(RULING)
            management.makeVertexLabel("opinion")
            //management.makeEdgeLabel("") //TODO
            stringProps.each {
                makeProperty(management, it, String.class)
            }
            //composite indexes - unique
            def hanji_id = makeProperty(management, 'hanji_id', String.class)
            def category = makeProperty(management, 'category', String.class)
            if (null == management.getGraphIndex('byHanjiAndCategoryUnique')) {
                management.buildIndex('byHanjiAndCategoryUnique', Vertex.class).addKey(hanji_id).addKey(category).unique().buildCompositeIndex()
            }

            //composite indexes
            attrIndexMap.each { key, value ->
                createSingleVertexCompositeIndex(management, key, value[0], value[1])
            }

            management.commit()
        } else {
            throw new IllegalArgumentException("only neo4j or titan allowed")
        }

    }

    private PropertyKey makeProperty(TitanManagement mgmt, String name, Class<?> clazz) {
         return mgmt.makePropertyKey(name).dataType(clazz).make()
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



    static TitanGraph openTitanGraph(File storageDirectory, int mutations) {
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
        conf.setProperty("storage.tupl.min-cache-size", "100000000") //TODO should this be a function of something?
        conf.setProperty("storage.tupl.map-data-files", "true")
        conf.setProperty("storage.tupl.direct-page-access", "false") //requires JNA which seems broken?
        final TitanGraph g = TitanFactory.open(conf)
        return g
    }
}
