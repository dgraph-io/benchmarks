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

curl localhost:8236/query -XPOST -d '{
 debug(_uid_: 15161013152876854722) {
  film.director.film {
   film.film.directed_by {
    film.director.film @filter(allof("type.object.name.en", "the")) {
     type.object.name.en
    }
   }
  }
 }
}' 2> /dev/null | python -m json.tool > data/allof.out

| grep object.name | wc -l > /tmp/out.txt

# This value has been verified for the golden set. See ../forward folder.
result=`cat /tmp/out.txt`
if [ $result != "25431" ]; then
	echo "Wrong number of results"
	killall dgraph
	exit 1
fi

curl localhost:8236/query -XPOST -d '{
 debug(_uid_: 15161013152876854722) {
  film.director.film {
   film.film.directed_by {
    film.director.film {
     type.object.name.en
    }
   }
  }
 }
}' 2> /dev/null | python -m json.tool | grep object.name | wc -l > /tmp/out.txt

curl localhost:8236/query -XPOST -d '{
 debug(_uid_: 15161013152876854722) {
  film.director.film {
   film.film.directed_by {
    film.director.film @filter(allof("type.object.name.en", "the a")) {
     type.object.name.en
    }
   }
  }
 }
}' 2> /dev/null | python -m json.tool | grep object.name


curl localhost:8236/query -XPOST -d '{
 debug(_uid_: 15161013152876854722) {
  film.director.film {
   film.film.directed_by {
    film.director.film @filter(allof("type.object.name.en", "the") && allof("type.object.name.en", "a")) {
     type.object.name.en
    }
   }
  }
 }
}' 2> /dev/null | python -m json.tool | grep object.name

# This value has been verified for the golden set. See ../forward folder.
result=`cat /tmp/out.txt`
if [ $result != "138676" ]; then
	echo "Wrong number of results"
	killall dgraph
	exit 1
fi

killall dgraph

popd &> /dev/null

echo "Tests passed!"