#/bin/bash

n=$1
f=$2
c=$3

filename="${n}P${f}F${c}C.csv"

python3 blobs.py $n $f $c $filename && \
hadoop fs -put -f $filename project/in
wait
hadoop fs -rm -r project/out/
wait
xmlstarlet ed -L -u "//property[name='dimentionality']/value" -v ${f} config.xml && \
xmlstarlet ed -L -u "//property[name='k']/value" -v ${c} config.xml
wait
hadoop jar ./target/MapReduce-1.0.jar project/in/$filename project/out
wait
hadoop fs -get -f project/out/initial.txt ./ && \
hadoop fs -get -f project/out/final.txt ./ && \
hadoop fs -get -f project/out/info.txt ./ && \
clear && \
python3 pytest.py $n $f $c $filename
