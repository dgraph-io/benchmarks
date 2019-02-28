#!/bin/bash

curl 10.240.0.10:8080/query -XPOST -d '
{
  actors(func: has(actor.film)) {
    uid
  }
}' | python -m json.tool \
   | jq '.data.actors[] | .uid' \
   | sed 's/"//g' > listofactors_uid
