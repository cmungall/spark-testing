#!/bin/sh
echo N=$1
spark-submit --class "SparkRunner" --master local[$1] target/scala-2.11/simple-project_2.11-1.0.jar
