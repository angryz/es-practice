package info.noconfuse.practice.es;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * @author Zheng Zhipeng
 */
public class Hello {

    public static void main(String[] args) throws Exception {
        Node node = nodeBuilder().build();
        Client client = node.client();
        //Settings settings = ImmutableSettings.settingsBuilder()
        //        .put("cluster.name", "elasticsearch").build();
        //TransportClient client = new TransportClient(settings);
        //client.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9200));
        UpdateResponse updateResponse = client.prepareUpdate("us", "tweet", "1")
                .setDoc(jsonBuilder().startObject()
                        .field("name", "John")
                        .field("date", new Date())
                        .field("tweet", "Elasticsearch and I have left the honeymoon stage, and I still love her.")
                        .field("user_id", 99).endObject())
                .execute().actionGet();
        System.out.println("_index:" + updateResponse.getIndex());
        System.out.println("_type: " + updateResponse.getType());
        System.out.println("_id: " + updateResponse.getId());
        System.out.println("_version: " + updateResponse.getVersion());
        System.out.println("_created: " + updateResponse.isCreated());

        IndexResponse response = client.prepareIndex("us", "tweet", "1")
                .execute().actionGet();
        System.out.println("_index: " + response.getIndex());
        System.out.println("_type: " + response.getType());
        System.out.println("_id: " + response.getId());
        System.out.println("_version: " + response.getVersion());
        System.out.println("_created: " + response.isCreated());

        client.close();
    }
}
