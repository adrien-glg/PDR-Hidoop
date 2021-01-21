POUR LES TESTS EN LOCAL SEULEMENT!

Avant de pouvoir effectuer le map reduce, on doit compiler, lancer les serveurs, lancer les Workers et enfin lancer le découpage du fichier. Pour cela simplement faire :
bash lancerServerWorkerWrite.sh <nombre de serveurs> <nom du fichier à couper> 

NB: Attention il faut vérifier que le nombre de serveur est au moins égal au nombre de serveurs dans le fichier config.Config 

Ensuite pour lancer le map reduce faire:
bash lancerMP.sh <nom du fichier>
