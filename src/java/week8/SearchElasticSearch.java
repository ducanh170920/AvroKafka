package week8;


import org.apache.http.HttpHost;
import org.apache.kerby.kerberos.kerb.client.preauth.pkinit.ClientConfiguration;
import org.apache.lucene.util.QueryBuilder;
import org.apache.spark.network.client.TransportClient;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchElasticSearch {


    public static void main(String[] args) throws IOException {
        String input = "Anh";

        // init connect
        RestHighLevelClient client = new RestHighLevelClient(
                                        RestClient.builder(new HttpHost("10.140.0.13",9200,"http")));


        // create request
        SearchRequest searchRequest = new SearchRequest("suggest_anhhd25");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders
                                                    .completionSuggestion("suggest_title")
                                                    .text(input);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        searchRequest.source(searchSourceBuilder);

        // create response
        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        Suggest suggest = searchResponse.getSuggest();

        List<String> suggestions = searchResponse.getSuggest()
                                        .filter(CompletionSuggestion.class).stream()
                                        .map(CompletionSuggestion::getOptions)
                                        .flatMap(Collection::stream)
                                        .map(option -> option.getText().string())
                                        .distinct()
                                        .collect(Collectors.toList());

        System.out.println(suggestions.toString());
        client.close();
    }
}
