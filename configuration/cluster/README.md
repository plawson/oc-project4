# Server VMs

## kube-node07
This is the master VM. It run the HDFS Namenode and the Yarn Resource Manager. It has complete Hadoop, Yarn and spark configuration.
### Distribution
The Hadoop distribution is installed in **/home/kubernetes/code/hadoop-2.8.4**.

The Spark distribution is installed in **/home/kubernetes/code/spark-2.3.1-bin-hadoop2.7**.
### Cluster life cycle commands
To start the cluster, proceed as follows:
```console
$ cd /home/kubernetes/code/hadoop-2.8.4/sbin
$ ./hadoop-daemon.sh start namenode
$ ./hadoop-daemons.sh start datanode
$ ./yarn-daemon.sh start resourcemanager
$ ./yarn-daemons.sh start nodemanager
```
Run the following commands to stop the cluster:
```console
$ cd /home/kubernetes/code/hadoop-2.8.4/sbin
$ ./yarn-daemons.sh stop nodemanager
$ ./yarn-daemon.sh stop resourcemanager
$ ./hadoop-daemons.sh stop datanode
$ ./hadoop-daemon.sh stop namenode
```
### Monitoring URLs
To monitor applications running on the Yarn cluster, use the below URL:

[http://kube-node07:8088](http://kube-node07:8088)

The Spark history server has been enabled. To access detail information about completed and running Spark applications, use the following URL:

[http://kube-node07:18080](http://kube-node07:18080)

## kube-node08
This VM is a HDFS Datanode and a Yarn Node manager. The Hadoop distribution is installed in **/home/kubernetes/code/hadoop-2.8.4**.
## kube-node09
This VM is a HDFS Datanode and a Yarn Node manager. The Hadoop distribution is installed in **/home/kubernetes/code/hadoop-2.8.4**.
## kube-node10
This VM is a HDFS Datanode and a Yarn Node manager. The Hadoop distribution is installed in **/home/kubernetes/code/hadoop-2.8.4**.