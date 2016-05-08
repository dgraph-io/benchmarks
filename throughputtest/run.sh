./throughputtest --numsec 50 --ip "http://10.240.0.10:8080/query"  --numuser 1000
./throughputtest --numsec 10 --ip "http://10.240.0.10:8080/query"  --numuser 1
./throughputtest --numsec 10 --ip "http://10.240.0.10:8080/query"  --numuser 10
./throughputtest --numsec 10 --ip "http://10.240.0.10:8080/query"  --numuser 50
./throughputtest --numsec 10 --ip "http://10.240.0.10:8080/query"  --numuser 100
./throughputtest --numsec 10 --ip "http://10.240.0.10:8080/query"  --numuser 500
./throughputtest --numsec 10 --ip "http://10.240.0.10:8080/query"  --numuser 1000

