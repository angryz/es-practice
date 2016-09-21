package info.noconfuse.practice.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Zheng Zhipeng
 */
public class Admin {

    protected static Client getClient() {
        try {
            return TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static class IndicesAdmin {

        public static void main(String[] args) {
            Client client = getClient();
            AdminClient adminClient = client.admin(); // admin() method returns an AdminClient
            IndicesAdminClient iClient = adminClient.indices();

            // create index
            iClient.prepareCreate("twitter").get();

            client.close();
        }
    }
}
