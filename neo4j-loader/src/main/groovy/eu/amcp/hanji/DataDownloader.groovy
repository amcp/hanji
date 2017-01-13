package eu.amcp.hanji

import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import com.google.common.collect.Lists

/**
 * Created by amcp on 2017/01/12.
 */
class DataDownloader {
    static void main(String[] args) {
        def dynamodb = new AmazonDynamoDBClient()
        dynamodb.withRegion(Regions.AP_NORTHEAST_1)
        def table = new Table(dynamodb, "case_data")
        def data = Lists.newArrayList(table.scan())
        Item item = new Item()
        def i = 1
        for(Item datum : data) {
            item.withMap(i.toString(), datum.asMap())
            i++
        }
        def file2 = new File('hanji.json')
        file2.text = item.toJSONPretty()
        System.exit(0);
    }
}
