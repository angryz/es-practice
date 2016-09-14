package info.noconfuse.practice.es;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * <pre>
 * - ES versin: 2.3.5
 * - ES host: localhost
 * - ES port: 9300 (default)
 * - ES cluster name: elasticsearch (default)
 * </pre>
 *
 * @author Zheng Zhipeng
 */
public class Hello {

    protected static Client getClient() throws UnknownHostException {
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName("127.0.0.1"), 9300));
    }

    private static class IndexApi {

        public static void main(String[] args) throws Exception {
            Client client = getClient();

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

            client.close();
        }
    }

    private static class GetApi {

        public static void main(String[] args) throws Exception {
            Client client = getClient();

            GetResponse response = client.prepareGet("us", "tweet", "12")
                    .setOperationThreaded(false)
                    .get();
            System.out.println("_index: " + response.getIndex());
            System.out.println("_type: " + response.getType());
            System.out.println("_id: " + response.getId());
            System.out.println("_version: " + response.getVersion());
            System.out.println("_source: " + response.getSourceAsString());

            client.close();
        }
    }

    private static class UpdateApi {

        public static void main(String[] args) throws Exception {
            Client client = getClient();

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            String today = df.format(new Date());
            UpdateResponse response = client.prepareUpdate("us", "tweet", "1")
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("date", today)
                            .endObject()).get();
            System.out.println("_index: " + response.getIndex());
            System.out.println("_type: " + response.getType());
            System.out.println("_id: " + response.getId());
            System.out.println("_version: " + response.getVersion());

            client.close();
        }
    }

    private static class DeleteApi {

        public static void main(String[] args) throws Exception {
            Client client = getClient();

            DeleteResponse response = client.prepareDelete("us", "tweet", "1").get();
            System.out.println("_index: " + response.getIndex());
            System.out.println("_type: " + response.getType());
            System.out.println("_id: " + response.getId());
            System.out.println("_version: " + response.getVersion());

            client.close();
        }
    }

    private static class MultiGetApi {

        public static void main(String[] args) throws Exception {
            Client client = getClient();

            MultiGetResponse response = client.prepareMultiGet()
                    .add("us", "tweet", "4", "6", "12")
                    .add("us", "user", "1")
                    .add("megacrop", "employee", "1")
                    .get();

            for (MultiGetItemResponse item : response) {
                GetResponse resp = item.getResponse();
                if (resp.isExists()) {
                    System.out.println("_index: " + resp.getIndex());
                    System.out.println("_type: " + resp.getType());
                    System.out.println("_id: " + resp.getId());
                    System.out.println("_version: " + resp.getVersion());
                    System.out.println("_source: " + resp.getSourceAsString());
                }
            }

            client.close();
        }
    }

    private static class SearchApi {

        public static void main(String[] args) throws Exception {
            Client client = getClient();

            SearchResponse response = client.prepareSearch("us", "megacrop")
                    .setTypes("tweet", "employee")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("about", "love"))
                    .setPostFilter(QueryBuilders.rangeQuery("age").from(25).to(35))
                    .setFrom(0)
                    .setSize(20)
                    .setExplain(true)
                    .execute()
                    .actionGet();
            System.out.println("total: " + response.getHits().totalHits());
            System.out.println("maxScore: " + response.getHits().maxScore());
            System.out.println("took: " + response.getTookInMillis());
            for (SearchHit hit : response.getHits().hits()) {
                System.out.println("_index: " + hit.getIndex() + ", _type: " + hit.getType()
                        + ", _id: " + hit.getId());
                System.out.println("_source: " + hit.getSourceAsString());
            }

            client.close();
        }
    }
}
