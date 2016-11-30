# NOT A RUNNABLE SCRIPT
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
# Verify that count is 138676 with Go script.
cat data/basic.out | grep object | wc -l

curl localhost:8236/query -XPOST -d @data/allof_the.in 2> /dev/null | python -m json.tool > data/allof_the.out
# Verify that count is 25431 with Go script.
cat data/allof_the.out | grep object | wc -l

curl localhost:8236/query -XPOST -d @data/allof_the_a.in 2> /dev/null | python -m json.tool > data/allof_the_a.out
# Verify that count is 367 with Go script.
cat data/allof_the_a.out | grep object | wc -l

curl localhost:8236/query -XPOST -d @data/anyof_the_a.in 2> /dev/null | python -m json.tool > data/anyof_the_a.out
# Verify that count is 28029 with Go script.
cat data/anyof_the_a.out | grep object | wc -l

curl localhost:8236/query -XPOST -d @data/allof_the_count.in 2> /dev/null | python -m json.tool > data/allof_the_count.out
# Check that the counts add up to 25431.
cat data/allof_the_count.out | grep count | paste -sd+ | bc

curl localhost:8236/query -XPOST -d @data/allof_the_first.in 2> /dev/null | python -m json.tool > data/allof_the_first.out
cat data/allof_the_first.out | grep object | wc -l
# To check this, we use allof_the_count and sum min(count, 10). Use Python.
cat data/allof_the_count.out | grep count | cut -f2 -d: > /tmp/a.txt
with open('/tmp/a.txt') as f:
  a = f.read().splitlines()
  a = [min(int(x), 10) for x in a]
  print sum(a)
# The answer is 4383 and they match up.

killall dgraph

popd &> /dev/null

echo "Tests passed!"