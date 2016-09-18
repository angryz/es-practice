package info.noconfuse.practice.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Zheng Zhipeng
 */
public class QueryDSL {

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

    protected static SearchResponse search(Client client, QueryBuilder queryBuilder) {
        System.out.println("\n>>>> Start searching => " + queryBuilder.toString());
        return client.prepareSearch("us")
                .setTypes("tweet")
                .setQuery(queryBuilder)
                .setFrom(0)
                .setSize(20)
                .setExplain(true)
                .execute()
                .actionGet();
    }

    protected static void printResponse(SearchResponse response) {
        System.out.println("total: " + response.getHits().totalHits());
        System.out.println("maxScore: " + response.getHits().maxScore());
        System.out.println("took: " + response.getTookInMillis());
        for (SearchHit hit : response.getHits().hits()) {
            System.out.println("_index: " + hit.getIndex() + ", _type: " + hit.getType()
                    + ", _id: " + hit.getId() + ", _score: " + hit.getScore());
            System.out.println("_source: " + hit.getSourceAsString());
        }
    }

    private static class MatchAll {

        public static void main(String[] args) {
            Client client = getClient();

            QueryBuilder qb = QueryBuilders.matchAllQuery();
            SearchResponse response = search(client, qb);
            printResponse(response);

            client.close();
        }
    }

    private static class FullTextSearch {

        public static void main(String[] args) {
            Client client = getClient();

            QueryBuilder match = QueryBuilders.matchQuery("tweet", "John elasticsearch");
            SearchResponse response = search(client, match);
            printResponse(response);

            QueryBuilder multiMatch = QueryBuilders.multiMatchQuery("John elasticsearch", "name",
                    "tweet");
            SearchResponse response1 = search(client, multiMatch);
            printResponse(response1);

            QueryBuilder commonTerms = QueryBuilders.commonTermsQuery("tweet", "John " +
                    "elasticsearch");
            SearchResponse response2 = search(client, commonTerms);
            printResponse(response2);

            QueryBuilder queryString = QueryBuilders.queryStringQuery("+John -elasticsearch");
            SearchResponse response3 = search(client, queryString);
            printResponse(response3);

            QueryBuilder simpleQueryString = QueryBuilders.simpleQueryStringQuery("+John " +
                    "-elasticsearch");
            SearchResponse response4 = search(client, simpleQueryString);
            printResponse(response4);

            client.close();
        }
    }

}
