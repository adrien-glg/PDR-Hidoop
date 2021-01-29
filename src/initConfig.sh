input="liste_ordi.txt"
test=$PWD'/config/Config.java'
if [[ -f "$test" ]]; then
    rm "$test"
fi
let "nbLignes= `cat $input | wc -l`"
let "i=0"


echo 'package config;

public class Config {

	public static String PATH = "/tmp/data/";' >> "$test"
echo $nbLignes
printf '
    public static String tab_serveurs[] = {' >> "$test"
while IFS= read -r line
do
  let "i+=1"
  printf '"' >> "$test"
  printf "$line" >> "$test"
  printf '"' >> "$test"
  if [[ "$i" -lt "$nbLignes" ]] ; then
    printf ', ' >> "$test"
  fi
done < "$input"
echo '};' >> "$test"


printf '
    public static int tab_ports[] = {' >> "$test"
let "port=2000"
for k in $(seq 1 $nbLignes)
do
    let "port+=1"
    printf "$port" >> "$test"
    if [[ "$k" -lt "$nbLignes" ]] ; then
    printf ', ' >> "$test"
  fi
done
echo '};' >> "$test"

printf '
    public static String PATH_SERVER[] = {' >> "$test"

for k in $(seq 1 $nbLignes)
do
    printf '"' >> "$test"
    printf '/tmp/serv' >> "$test"
    printf '"' >> "$test"
    if [[ "$k" -lt "$nbLignes" ]] ; then
        printf ', ' >> "$test"
    fi
    
done
echo '};' >> "$test"

echo '}' >> "$test"
 


