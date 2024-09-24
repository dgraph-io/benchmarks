
Dataset created to support the blog post about variable propagation on 'social' use cases.

https://dgraph.io/blog/post/20240923-variable-propgation/


The RDF file `contacts.rdf.gz` has been generated for 10000 users with the script generate.py. 


Start the dgraph container with the following command

> docker run -it -d -p 8080:8080 -p 9080:9080 -v /path/to/dgraph-data:/dgraph --name dgraph-dev dgraph/standalone:latest

Copy the files to the mounted directory so that they are seen in Docker.

> cp contacts.rdf.gz <local path to /dgraph-data>
> cp contacts.schema <local path to /dgraph-data>

Use dgraph live command in the docker instance

> docker exec -it dgraph-dev dgraph live -c 1 -f /dgraph/contacts.rdf.gz -s /dgraph/contacts.schema

You cat get some usernames 

```graphql
{
  user(func:has(username), first:5) {
    username
}
```

and test the queries from the blog post
## mutual 'follows'
```graphql
{

userA as var(func: eq(username, "barbara10")) { 
  # use a named variable userA to be able to exclude this node later in the query
    c as math(1) # start c =1 on user A Node
    follows_of_userA as follows {
        # c is propagated, each follow is reached one time so c =1 for every follow
      ~follows @filter(NOT uid(userA)) {
        # ~follows is the reverse relationship
        # users at this level are reached by all the common follows, 
        # c = sum all path = count of common follows
        # keep the value in a variable, 
        # in Dgraph a variable is a map uid -> value, so we have the count for every target
                mutual_follows as math(c)
      }
    }
  }
    
  target_user(func: uid(mutual_follows), orderdesc: val(mutual_follows), first:1) {
    username
    mutual_follows_count: val(mutual_follows)
    mutual_follows: follows @filter(uid(follows_of_userA)) {
      username
    }
  }
}
```

## mutual 'contacts'
```graphql
{
  var(func: eq(username, "barbara10")) {
    c as math(1)
    userA_phone_number as ~belongs_to {
      userA_contacts as has_in_contacts {
        ~has_in_contacts @filter(NOT uid(userA_phone_number)) {
          belongs_to{
              mutual_contacts as Math(c)
          }
        }
      }
    }
  }
  
  
  target_user(func: uid(mutual_contacts), orderdesc: val(mutual_contacts), first: 1) {
    username
    mutual_contact_count:val(mutual_contacts)
    phone:~belongs_to {
      phone_number
      mutual_contacts: has_in_contacts @filter(uid(userA_contacts))  {
        phone_number 
        belongs_to {
          username
        }
      }
    }
  }
}
```

## computing a complex score

```graphql
{
  userA as var(func: eq(username, "barbara10")) { 
    c as math(1) # start c =1 on user A Node
    # first block to compute mutual follows using variable propagation
    follows {
      ~follows @filter(NOT uid(userA)) {
                mutual_follows as math(c)
      }
    }
    # second block to compute mutual contacts using same variable !
    #  different path.
    userA_phone_number as ~belongs_to {
      has_in_contacts {
        ~has_in_contacts @filter(NOT uid(userA_phone_number)) {
          belongs_to{
              mutual_contacts as Math(c)
          }
        }
      }
    }
  }

# compute a score using the formula
  var(func: uid(mutual_follows, mutual_contacts)) {
    score as math(0.4 * mutual_follows + 0.6 * mutual_contacts)
  }
# get target info
  target(func: uid(score), orderdesc: val(score), first: 1) {
    username
    score: val(score)
    count_mutual_follows: val(mutual_follows)
    count_mutual_contacts: val (mutual_contacts)
  }
}
```