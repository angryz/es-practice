package info.noconfuse.practice.es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Zheng Zhipeng
 */
public class Hello {

    public static void main(String[] args) throws Exception {
        Client client = TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName("127.0.0.1"), 9300));

        // Index API
        IndexResponse response = client.prepareIndex("us", "tweet", "1")
                .setSource(
                        jsonBuilder().startObject()
                                .field("name", "John")
                                .field("tweet", "Elasticsearch and I have left the honeymoon stage, and I still love her.")
                                .field("user_id", 1)
                                .field("date", new Date())
                                .endObject()
                )
                .execute().actionGet();
        System.out.println("_index: " + response.getIndex());
        System.out.println("_type: " + response.getType());
        System.out.println("_id: " + response.getId());
        System.out.println("_version: " + response.getVersion());
        System.out.println("_created: " + response.isCreated());

        // Update API

        //UpdateResponse updateResponse = client.update(uReq).get();
        //System.out.println("_index:" + updateResponse.getIndex());
        //System.out.println("_type: " + updateResponse.getType());
        //System.out.println("_id: " + updateResponse.getId());
        //System.out.println("_version: " + updateResponse.getVersion());
        //System.out.println("_created: " + updateResponse.isCreated());

        client.close();
    }
}
