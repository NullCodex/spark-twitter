package stream;

import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public final class SparkTwitter {
    private static String INPUT_DIRECTORY;
    private static String CHECKPOINT_DIRECTORY;
    private static boolean CHECKPOINT_ENABLE;
    private static String IP_ADDRESS = "127.0.0.1";
    private static int IP_PORT;
    private static boolean OUTPUT_TO_SOCKET;
    private static int BATCH_INTERVAL, WINDOW_INTERVAL, WINDOW_LENGTH;

    public static void main(String[] args) {
        //specify what arguments are accepted
        if (args.length >= 5) {
            INPUT_DIRECTORY = args[0];
            CHECKPOINT_DIRECTORY = args[1] + "_check";
            CHECKPOINT_ENABLE = true;
            BATCH_INTERVAL = Integer.parseInt(args[2]);
            WINDOW_INTERVAL = Integer.parseInt(args[3]);
            WINDOW_LENGTH = Integer.parseInt(args[4]);
        } else {
            System.out.println("required arguments: input_dir checkpoint_dir batch_interval window_interval window_length");
            System.exit(-1);
        }


        SparkConf conf = new SparkConf().setAppName("twitter streaming");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(BATCH_INTERVAL));

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);


        if (CHECKPOINT_ENABLE) {
            streamingContext.checkpoint(CHECKPOINT_DIRECTORY);
        }

        JavaDStream<String> tweets = streamingContext.textFileStream(INPUT_DIRECTORY);

        sparkStreaming(tweets);

        streamingContext.start();

        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("streamingContext.awaitTermination() was interrupted");
        }
    }


    public static void sparkStreaming(JavaDStream<String> tweets) {
        // Split each line into words
        JavaDStream<String> words = tweets.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> hashtags = words.filter(s -> {
            return s.indexOf('#') != -1;
        }).mapToPair(s -> new Tuple2<>(s, 1));

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                        Integer newSum = state.or(0);

                        //         System.out.println(values);
                        for(int i : values)
                        {
                            newSum += i;
                        }
                        return Optional.of(newSum);
                    }
                };

        JavaPairDStream<String, Integer> runningCounts =
                hashtags.updateStateByKey(updateFunction);

        runningCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {

                List<Pair<String, Integer>> pairs = new ArrayList<>();
                List<String> tags = new ArrayList<>();
                List<Integer> counts = new ArrayList<>();

                Map<String, Integer> valuePairs = stringIntegerJavaPairRDD.collectAsMap();
                for (String key : valuePairs.keySet()) {
                    pairs.add(new Pair<>(key, valuePairs.get(key)));
                }

                Collections.sort(pairs, new Comparator<Pair<String, Integer>>() {
                    @Override
                    public int compare(final Pair<String, Integer> o1, final Pair<String, Integer> o2) {
                        return o2.getValue().compareTo(o1.getValue());
                    }
                });

                for (int i = 0; i < 10 && i < pairs.size(); i++) {
                    tags.add(pairs.get(i).getKey());
                    counts.add(pairs.get(i).getValue());
                }

                HttpClient client = HttpClientBuilder.create().build();
                HttpPost post = new HttpPost("http://localhost:5001/updateData");
                List<NameValuePair> arguments = new ArrayList<>(2);
                arguments.add(new BasicNameValuePair("label", tags.toString()));
                arguments.add(new BasicNameValuePair("data", counts.toString()));

                try {
                    post.setEntity(new UrlEncodedFormEntity(arguments));
                    HttpResponse response = client.execute(post);

                    // Print out the response message
                    System.out.println(EntityUtils.toString(response.getEntity()));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        });
    }
}