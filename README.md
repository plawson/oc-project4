# oc-project4
Analysez des données en batch

## Contenu des répertoires
* analysis : Ce répertoire contient le script python d'analyse du master dataset pour séléctionner les
experts sur un sujet donné.
* configuration : Contient les fichiers Hadoop et Spark qui ont été adaptés pour la configuration du
cluster. Procédure de lancement et d'arrêt du cluster.
* datalake : Les scripts de sérialisation (et desérialization) pour la création du master dataset.
* documents : Livrables du projet et slides de présentation.
* shl : 
     * Script de création de la structure de répertoire HDFS.
     * Script de suppression du répetoire de test dans le cluster HDFS
     * Script de lancement de l'analyse
 
 ## Arborescence du Datalake
* /data/frwiki
* /data/frwiki/raw
* /data/frwiki/frwiki-20180501
* /data/frwiki/frwiki-20180501/master
* /data/frwiki/frwiki-20180501/master/history.avsc
* /data/frwiki/frwiki-20180501/master/pagelinks.avsc
* /data/frwiki/frwiki-20180501/master/full
* /data/frwiki/frwiki-20180501/master/test