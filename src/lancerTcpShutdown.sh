#Pour executer ce script il faut avoir deja installé les fichiers du source dans les serveurs 
input="liste_ordi.txt"
#Liste du path du projet dans chaque serveur
let "nbLignes= `cat $input | wc -l`"
let "i=0"
let "portServ=2000"
let "portWork=3000"
path="$PWD"
while IFS= read -r line
do
    let "portServ+=1"
    let "portWork+=1"
    #Fermeture des ports TCP par ssh 
    echo  "Fermeture des ports $portServ et $portWork sur la machine $line"
    ssh $USER@$line "cd $path;bash tcpShutdown.sh $portServ $portWork" &
done < "$input"
echo "fini"
