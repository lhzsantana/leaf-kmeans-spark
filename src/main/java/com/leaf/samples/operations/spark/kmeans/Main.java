package com.leaf.samples.operations.spark.kmeans;

import com.google.gson.Gson;
import com.leaf.samples.operations.spark.kmeans.models.Authentication;
import com.leaf.samples.operations.spark.kmeans.models.Operation;
import com.leaf.samples.operations.spark.kmeans.models.Operations;
import com.leaf.samples.operations.spark.kmeans.models.Token;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.FeatureHasher;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;

public class Main {

    SparkSession spark = SparkSession
        .builder()
        .appName("Leaf API with Spark Kmeans")
        .config("spark.master", "local")
        .getOrCreate();


    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
    String api = "https://api.withleaf.io";
    //CHANGE IT
    String username = "";
    String password = "";

    public static void main(String [] args) throws IOException, InterruptedException {

        Main main = new Main();

        String token = main.authenticate();

        for(Operation operation : main.getStandardGeoJSON(token)) {
            Dataset<Row> rawDataset = main.loadData(operation.getStandardGeojson());
            Dataset<Row> featurized = main.featurize(rawDataset);

            KMeansModel model = main.calculateKMeans(featurized);

            // Shows the result.
            Vector[] centers = model.clusterCenters();
            System.out.println("Cluster Centers: ");
            for (Vector center: centers) {
                System.out.println(center);
            }

            Dataset<Row> predictions = main.runPredictions(model, featurized).drop("features");

            predictions.write().json("output-"+operation.getId()+".json");
        }
    }

    private  Dataset<Row> runPredictions(KMeansModel model, Dataset<Row> featurized){

        Dataset<Row> predictions = model.transform(featurized);

        predictions.show(100);

        System.out.println("----------------");

        return predictions;
    }

    private KMeansModel calculateKMeans(Dataset<Row> dataset){

        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(30).setSeed(1L);
        KMeansModel model = kmeans
            .fit(dataset);

        // Make predictions
        return model;
    }

    private Dataset<Row> featurize(Dataset<Row> raw){

        Dataset<Row> properties = raw
            .withColumn("yieldVolume", raw.col("properties.yieldVolume"))
            .withColumn("wetMass", raw.col("properties.wetMass"))
            .withColumn("moisture", raw.col("properties.moisture"));

        properties.printSchema();

        FeatureHasher hasher = new FeatureHasher()
            .setInputCols(new String[]{"yieldVolume", "wetMass", "moisture"})
            .setOutputCol("features");

        Dataset<Row> featurized = hasher.transform(properties);

        featurized.printSchema();
        featurized.show(100);

        return featurized;
    }

    private Dataset<Row> loadData(String stdPath) throws IOException {

        String path = "/tmp/file.json";

        InputStream in = new URL(stdPath).openStream();
        Files.copy(in, Paths.get(path), StandardCopyOption.REPLACE_EXISTING);

        Dataset<Row> operations = spark
            .read()
            .json(path);

        return operations;
    }

    private List<Operation> getStandardGeoJSON(String token) throws IOException, InterruptedException {

        HttpRequest request = HttpRequest.newBuilder(URI.create(api+"/services/operations/api/files?page=0&size=3&status=processed"))
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer "+token)
            .GET().build();

        HttpResponse<String> response = client.send(request,HttpResponse.BodyHandlers.ofString());

        return (new Gson()).fromJson(response.body(), Operations.class).getOperations();
    }

    private String authenticate() throws IOException, InterruptedException {

        Authentication authentication = new Authentication();
        authentication.setUsername(username);
        authentication.setPassword(password);

        HttpRequest request = HttpRequest.newBuilder(URI.create(api+"/api/authenticate"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString((new Gson()).toJson(authentication))).build();

        HttpResponse<String> response = client.send(request,HttpResponse.BodyHandlers.ofString());

        return (new Gson()).fromJson(response.body(), Token.class).getId_token();
    }
}
