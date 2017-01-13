package eu.amcp.hanji

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph
import org.apache.tinkerpop.gremlin.structure.Graph
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.graphdb.GraphDatabaseService
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
        try {
            new MetadataLoader(storageDirectory)
        } catch(Exception e) {
            e.printStackTrace()
            System.exit(1)
        }
        System.exit(0)
    }

    IndexDefinition indexNeoProperty(Schema schema, String label, String property) {
        return schema.indexFor(DynamicLabel.label(label)).on(property).create()
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

        def neo4j = Neo4jGraph.open(dir.getAbsolutePath())
        graph = neo4j
        def baseGraph = (Neo4jGraphAPIImpl) neo4j.getBaseGraph()
        Field f = baseGraph.getClass().getDeclaredField("db"); //NoSuchFieldException
        f.setAccessible(true);
        GraphDatabaseService base = (GraphDatabaseService) f.get(baseGraph); //IllegalAccessException
        def tx = neo4j.tx()
        try {
            def schema = base.schema()
            schema.constraintFor(DynamicLabel.label(RULING)).assertPropertyIsUnique("hanji_id_category").create()
            attrIndexMap.each { key, value ->
                indexNeoProperty(schema, RULING, key)
            }

            tx.commit()
        } catch(Exception e) {
            tx.rollback()
        }
    }
}
