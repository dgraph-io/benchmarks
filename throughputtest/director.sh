#!/bin/bash

curl 10.240.0.10:8080/query -XPOST -d '
{
  directors(func: has(director.film)) {
    uid
  }
}' | python -m json.tool \
   | jq '.data.directors[] | .uid' \
   | sed 's/"//g' > listofdirectors_uid
