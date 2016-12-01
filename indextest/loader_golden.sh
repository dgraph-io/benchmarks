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

############################### Filter using allof, anyof
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
# For simplicity, just check in python.

X=allof_the_first
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep object | wc -l  # 4383



with open('data/allof_the_count.out') as f:
	a = f.read().splitlines()
a = [s for s in a if 'count' in s]
b = [int(s.strip().split(':')[1].replace('"', '').strip()) for s in a]
c = [min([s, 10]) for s in b]
print sum(b)  # Should be 25431
print sum(c)  # Should be 4383





############################### Sort by release dates
X=releasedate
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep release | wc -l
# Check that the answer is 137858. We found this from Go script.

# Next we try the sorting.
X=releasedate_sort
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep release | wc -l  # Count is 137858.
# Eyeballed the results. They look sorted.

# Get counts.
X=releasedate_sort_count
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out

# Apply pagination.
X=releasedate_sort_first_offset
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep release | wc -l  # 2315



with open('data/releasedate_sort_count.out') as f:
	a = f.read().splitlines()
a = [s for s in a if 'count' in s]
b = [int(s.strip().split(':')[1].replace('"', '').strip()) for s in a]
c = [min([max([s - 10, 0]), 5]) for s in b]
print sum(b)
print sum(c)  # Should be 2315



############################### Generator by anyof

# Get counts.
X=gen_anyof_good_bad
curl localhost:8236/query -XPOST -d @data/${X}.in 2> /dev/null | python -m json.tool > data/${X}.out
cat data/${X}.out | grep object | wc -l 
# 1103
# This is verified in forward.go.


killall dgraph

popd &> /dev/null

echo "Tests passed!"