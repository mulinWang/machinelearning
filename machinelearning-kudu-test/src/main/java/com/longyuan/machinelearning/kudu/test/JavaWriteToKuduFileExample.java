package com.longyuan.machinelearning.kudu.test;

import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import scala.collection.JavaConverters;

public class JavaWriteToKuduFileExample {

    private static final String KUDU_MASTER = System.getProperty(
            "kuduMaster", "quickstart.cloudera");

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("test kudu")
                .master("local[*]")
                .getOrCreate();

        KuduContext kuduContext = new KuduContext("172.16.249.38:7051",spark.sparkContext());
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        List<String> seq = new ArrayList<>();
        seq.add("name");

        //kuduContext.createTable("test",javaBeanDS.schema(),seq.asScala, new CreateTableOptions());
    }


    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
