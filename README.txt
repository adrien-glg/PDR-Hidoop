Pour utiliser Hidoop:

1- placer le fichier à traiter dans /tmp/data
1bis - se placer dans src/
2- mettre dans liste_ordi.txt les machines que l'on souhaite utiliser (une par ligne)
3- bash initConfig.sh 
4- bash runWorkerServers.sh
5- bash installFile.sh <nom du fichier à traité placé dans /tmp/data>
6- java application.MyMapReduce <nom du fichier à traité placé dans /tmp/data>
7- vous pouvez maintenant récupérer le résultat dans /tmp/data ! 
8 - IMPORTANT : bash lancerTcpShutdown.sh afin de fermer les ports TCP actifs
sur les machines remote.
