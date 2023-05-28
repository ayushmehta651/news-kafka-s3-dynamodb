package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.NewsEntity;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static constants.constants.NEWS_URL;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(NEWS_URL)).build();
        HttpClient client = HttpClient.newBuilder().build();
        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());
        ObjectMapper mapper = new ObjectMapper();
        NewsEntity newsEntity = mapper.readValue(response.body().toString(), NewsEntity.class);
        System.out.print(newsEntity.getArticles());
    }
}