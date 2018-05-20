#!/bin/bash

echo "starting spark streaming on local client..."

batch_interval=500
window_length=3000
window_interval=1000

input_dir="tweets_input"
checkpoint_dir="spark_twitter"

rm -rf $input_dir/*
rm -rf $checkpoint_dir*

spark-submit --class stream.SparkTwitter --master local[*] target/SparkTwitter-0.0.1-SNAPSHOT.jar $input_dir $checkpoint_dir $batch_interval $window_interval $window_length