#Pour executer ce script il faut avoir deja installé les fichiers du source dans les serveurs 
input="liste_ordi.txt"
#Liste du path du projet dans chaque serveur
let "nbLignes= `cat $input | wc -l`"
let "i=0"
let "port=2000"
path="$PWD"
javac */*.java
while IFS= read -r line
do
    let "port+=1"
    #Lancement du serveur par ssh 
    echo  "Initialisation du Serveur numero $i sur la machine $line et le port $port"
    ssh $USER@$line 'cd '$path';mkdir -p /tmp/serv; nohup java hdfs.HdfsServer '$i  &
    #Lancement du worker par ssh 
    echo  "Initialisation du Demon numero $i sur la machine $line et le port $port"
    ssh $USER@$line 'cd '$path';mkdir -p /tmp/serv;nohup java ordo.WorkerImpl '$i  &
    let "i+=1"
done < "$input"
echo "fini"

