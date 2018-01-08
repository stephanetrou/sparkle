[![Build Status](https://travis-ci.org/stephanetrou/sparkle.svg?branch=master)](https://travis-ci.org/stephanetrou/sparkle)


# Sparkle

_Sparkle_ is a framework largely inspired by SpringBoot that help Data Engineer write Spark Job. He only use 
librairies provided by Spark. 

## Exemple 

```java
    public class SparkleSample extends Job {
  
        public static void main(String args[]) {
            SparkleApplication.run(SparkleSample.class, args);
        }
        
        public run() {
            read(getSource())
                .as(csvh())
                .createOrReplaceTempView("train");
            
            description("query", "show \uD83E\uDD84");
            Dataset<Row> ds = spark().sql("SELECT sum(age) as sum_age, avg(age) as avg_age FROM train where is_not_blank(Cabin) group by sex");
            
            overwrite(ds).as(parquet(getDestination()));
        }
    }
``` 

## Features : 

* Simple CLI Configuration 
* Automatically register UDFs
* Add listener to monitor your job 
* Add messages in Spark UI
* Write Once Run Everywhere (Standalone, Cluster ...)
* For Windows no more winutils error
* Testing : Improve errors messages 
* Package FatJar (not yet)
* Create and chain subjobs (not yet)








