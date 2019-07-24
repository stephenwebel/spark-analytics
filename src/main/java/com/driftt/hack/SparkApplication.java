package com.driftt.hack;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class SparkApplication {

  private static final Logger log = LogManager.getLogger(SparkApplication.class);
  public static final String EVENT_HASH_KEY = "partitionKey";

  public static void main(String[] args) {
    JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(new SparkConf()
                                                                                         .setSparkHome("/home/ubuntu/spark_server/spark-2.4.3-bin-hadoop2.7")
                                                                                         .setMaster("spark://ip-10-20-88-24.ec2.internal:7077")
                                                                                         .setAppName("test2")));

    List<Long> endUserIds = Arrays.asList(3072226196L, 1009584946L, 3183966994L);

    JavaRDD<Long> distData = sc.parallelize(endUserIds);

    JavaRDD<List<String>> map = distData.map(id -> {
      DynamoDbClient dynamoDbClient = DynamoDbClient.create();
      Map<String, AttributeValue> vals = new HashMap<>();
      vals.put(":pKey", AttributeValue.builder()
          .s(String.valueOf(id))
          .build());
      QueryResponse query = dynamoDbClient.query(QueryRequest.builder()
                                                     .tableName("timeline-encrypted-v2-prod")
                                                     .keyConditionExpression(EVENT_HASH_KEY + " = :pKey")
                                                     .expressionAttributeValues(vals)
                                                     .build());
      return query.items().stream().map(i -> i.get(EVENT_HASH_KEY).s()).collect(Collectors.toList());
    });
    List<List<String>> mappedShit = map.collect();

    log.info("found sum shit: " + mappedShit);
  }


}
