set -e

SRC=`pwd`
DIR=$HOME/dgraphtest

mkdir -p $DIR

pushd $DIR &> /dev/null

if [ ! -f "goldendata.gz" ]; then
  wget https://github.com/dgraph-io/benchmarks/raw/master/data/goldendata.gz
fi

if [ ! -f "goldendata" ]; then
  gunzip -k goldendata.gz
fi

# Make sure there's no dirty data.
rm -Rf p w

cp -f $SRC/schema.txt ./

dgraphloader -debugmode -schema schema.txt -p p -debugmode -rdfgzips goldendata.gz

dgraph -p p -port 8236 -schema schema.txt &

echo "Wait for dgraph to start up"
sleep 5

curl localhost:8236/query -XPOST -d @data/basic.in 2> /dev/null | python -m json.tool > data/basic.out

curl localhost:8236/query -XPOST -d @data/allof_the.in 2> /dev/null | python -m json.tool > data/allof_the.out

curl localhost:8236/query -XPOST -d @data/allof_the_a.in 2> /dev/null | python -m json.tool > data/allof_the_a.out

curl localhost:8236/query -XPOST -d @data/allof_the_count.in 2> /dev/null | python -m json.tool > data/allof_the_count.out

# You can get number of objects by:
#  cat data/basic.out | grep object | wc -l
# Verified number of objects:
# basic: 138676
# allof_the: 25431
# allof_the_a: 367

killall dgraph

popd &> /dev/null

echo "Tests passed!"