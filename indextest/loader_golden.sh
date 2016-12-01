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

cp -f $SRC/schema.txt ./

# Make sure there's no dirty data.
rm -Rf p w

dgraphloader -debugmode -schema schema.txt -p p -debugmode -rdfgzips goldendata.gz

dgraph -p p -port 8236 -schema schema.txt &

echo "Wait for dgraph to start up"
sleep 5

X=basic
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
# Verify that count is 138676 with Go script.
cat data/${X}.out | grep object | wc -l

X=allof_the
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
# Verify that count is 25431 with Go script.
cat data/${X}.out | grep object | wc -l

X=allof_the_a
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
# Verify that count is 367 with Go script.
cat data/${X}.out | grep object | wc -l

X=anyof_the_a
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
# Verify that count is 28029 with Go script.
cat data/${X}.out | grep object | wc -l

X=allof_the_count
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
# Check that the counts add up to 25431.
cat data/${X}.out | grep count | paste -sd+ | bc

X=allof_the_first
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep object | wc -l
# To check this, we use allof_the_count and sum min(count, 10). Use Python.
cat data/allof_the_count.out | grep count | cut -f2 -d: > /tmp/a.txt
with open('/tmp/a.txt') as f:
  a = f.read().splitlines()
  a = [min(int(x), 10) for x in a]
  print sum(a)
# The answer is 4383 and they match up.

X=releasedate
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep release | wc -l
# Check that the answer is 137858. We found this from Go script.

# Next we try the sorting.
X=releasedate
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep release | wc -l

killall dgraph

popd &> /dev/null

echo "Tests passed!"