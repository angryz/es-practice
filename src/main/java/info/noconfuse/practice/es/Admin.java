package info.noconfuse.practice.es;

import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
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

            // delete indices
            if (iClient.prepareExists("twitter").get().isExists())
                iClient.prepareDelete("twitter").get();
            if (iClient.prepareExists("twitter-1").get().isExists())
                iClient.prepareDelete("twitter-1").get();

            // create index
            iClient.prepareCreate("twitter").get();
            System.out.println("Index 'twitter' created.");

            // index settings
            iClient.prepareCreate("twitter-1")
                    .setSettings(Settings.builder()
                            .put("index.number_of_shards", 3)
                            .put("index.number_of_replicas", 2))
                    .get();
            System.out.println("Index 'twitter-1' created.");

            client.close();
        }
    }
}
