echo "submitting spark job..."
# export EXAMPLE_JAR=$(kubectl exec -ti --namespace kafka-adsoftsito my-spark-worker-0 -- find examples/jars/ -name 'spark-example*\.jar' | tr -d '\r')  
kubectl exec -ti --namespace kafka-adsoftsito my-spark-worker-0 -- spark-submit --master spark://my-spark-master-svc:7077 --class org.apache.spark.examples.SparkPi https://raw.githubusercontent.com/adsoftsito/kafka-consumer-python/master/hello.py
