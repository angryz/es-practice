package info.noconfuse.practice.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.index.query.QueryBuilders.*;

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

    private static class TermLevelSearch {

        public static void main(String[] args) {
            Client client = getClient();

            QueryBuilder term = QueryBuilders.termQuery("tweet", "elasticsearch");
            printResponse(search(client, term));

            QueryBuilder terms = QueryBuilders.termsQuery("tweet", "elasticsearch", "mary");
            printResponse(search(client, terms));

            QueryBuilder range = QueryBuilders.rangeQuery("date")
                    .from("2014-09-20")
                    .to("2014-09-24")
                    .includeLower(true)
                    .includeUpper(false);
            // a simplified form
            //QueryBuilders.rangeQuery("date").gte("2014-09-20").lt("2014-09-24");
            printResponse(search(client, range));

            QueryBuilder exists = QueryBuilders.existsQuery("tweet");
            printResponse(search(client, exists));

            // missing query is deprecated in 2.2.0
            QueryBuilder missing = QueryBuilders.missingQuery("tweet")
                    .existence(true)
                    .nullValue(true);
            printResponse(search(client, missing));

            // instead of missing query
            QueryBuilder missingInstead = QueryBuilders.boolQuery().mustNot(
                    QueryBuilders.existsQuery("tweet"));
            printResponse(search(client, missingInstead));

            QueryBuilder prefix = QueryBuilders.prefixQuery("tweet", "elastic");
            printResponse(search(client, prefix));

            QueryBuilder wildcard = QueryBuilders.wildcardQuery("tweet", "el?st*");
            printResponse(search(client, wildcard));

            QueryBuilder regexp = QueryBuilders.regexpQuery("tweet", "el.*ch");
            printResponse(search(client, regexp));

            QueryBuilder fuzzy = QueryBuilders.fuzzyQuery("tweet", "elasticseargh");
            printResponse(search(client, fuzzy));

            QueryBuilder type = QueryBuilders.typeQuery("tweet");
            printResponse(search(client, type));

            QueryBuilder ids = QueryBuilders.idsQuery("tweet")
                    .addIds("4", "12");
            printResponse(search(client, ids));

            client.close();
        }
    }

    private static class CompoundQueries {

        public static void main(String[] args) {
            Client client = getClient();

            QueryBuilder constantScore = constantScoreQuery(
                    termQuery("tweet", "elasticsearch")
            ).boost(2.0f);
            printResponse(search(client, constantScore));

            QueryBuilder bool = boolQuery()
                    .must(termQuery("tweet", "elasticsearch"))
                    .must(rangeQuery("date").gte("2014-09-10"))
                    .mustNot(termQuery("tweet", "love"))
                    .should(termQuery("tweet", "API"));
            printResponse(search(client, bool));

            QueryBuilder disMax = disMaxQuery()
                    .add(termQuery("tweet", "elasticsearch"))
                    .add(termQuery("tweet", "mary"))
                    .boost(1.2f)
                    .tieBreaker(0.7f);
            printResponse(search(client, disMax));

            QueryBuilder functionScore = functionScoreQuery()
                    .add(
                            matchQuery("tweet", "elasticsearch"),
                            ScoreFunctionBuilders.randomFunction("ABCDEF")
                    );
            printResponse(search(client, functionScore));

            QueryBuilder boosting = boostingQuery()
                    .positive(termQuery("tweet", "elasticsearch"))
                    .negative(termQuery("tweet", "love"))
                    .negativeBoost(0.4f);
            printResponse(search(client, boosting));

            QueryBuilder indices = indicesQuery(
                    termQuery("tweet", "elasticsearch"),
                    "us", "megacrop"
            ).noMatchQuery(termQuery("about", "love"));
            System.out.println("\n>>>> Start searching => " + indices.toString());
            SearchResponse response = client.prepareSearch("us", "megacrop")
                    .setQuery(indices)
                    .setFrom(0)
                    .setSize(20)
                    .setExplain(true)
                    .execute()
                    .actionGet();
            printResponse(response);

            client.close();
        }
    }

    private static class SpanQueries {

        public static void main(String[] args) {
            Client client = getClient();

            QueryBuilder spanTerm = spanTermQuery("tweet", "elasticsearch");
            printResponse(search(client, spanTerm));

            QueryBuilder spanMultiTerm = spanMultiTermQueryBuilder(
                    prefixQuery("tweet", "elastic")
            );
            printResponse(search(client, spanMultiTerm));

            QueryBuilder spanFirst = spanFirstQuery(
                    spanTermQuery("tweet", "elasticsearch"),
                    1
            );
            printResponse(search(client, spanFirst));

            QueryBuilder spanNear = spanNearQuery()
                    .clause(spanTermQuery("tweet", "elasticsearch"))
                    .clause(spanTermQuery("tweet", "I"))
                    .clause(spanTermQuery("tweet", "love"))
                    .slop(12)
                    .inOrder(false)
                    .collectPayloads(false);
            printResponse(search(client, spanNear));

            client.close();
        }
    }
}
