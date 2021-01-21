let "i=$1-1"
javac src/*/*.java
for c in $(seq 0 $i)
do
    let "indiceWorker=$c+1"
    mkdir serv$indiceWorker
    mate-terminal -x bash -c "cd src;java ordo.WorkerImpl $indiceWorker;bash"
    mate-terminal -x bash -c "cd src;java hdfs.HdfsServer $c;bash"
    
done

cd src
java hdfs.HdfsClient write line $2
