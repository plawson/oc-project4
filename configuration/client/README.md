# Client VM

## kube-node11
This is the client VM used to submit Spark applications to the Yarn cluster.
It has minimum Hadoop and Spark configuration to be able to connect to the Yarn Resource Manager and submit the app.

### Environment
Set the HADOOP_CONF_DIR environment variable to point to the Hadoop configuration directory:
```console
$ export HADOOP_CONF_DIR=/home/kubernetes/code/hadoop-2.8.4/etc/hadoop
```
### Distribution
The Hadoop distribution is installed in **/home/kubernetes/code/hadoop-2.8.4**.

The Spark distribution is installed in **/home/kubernetes/code/spark-2.3.1-bin-hadoop2.7**.
