
```sh
$ gcloud iam service-accounts create tradegame-pubsub-sa \
  --description="Service Account for accessing PubSub from quotegame"

$ PROJECT=cloud-tradegame

# Directly put the sa as editor as we need to publish, subscribe and also create subscriptions.    
$ gcloud projects add-iam-policy-binding $PROJECT \
    --member=serviceAccount:tradegame-pubsub-sa@$PROJECT.iam.gserviceaccount.com\
    --role=roles/pubsub.editor
```

```sh
$ curl localhost:8080/api/quote/CYB
140.57                                                                                                                                                                                              

$ curl localhost:8080/api/quote/TYR
187.71

$ curl localhost:8080/api/user -XPOST -H 'Content-type: application/json' \
  -d '{"name":"lbroudoux", "email":"lbroudoux@google.com"}' -s | jq
  
$ curl localhost:8080/api/portfolio -s | jq
[
  {
    "username": "lbroudoux",
    "money": 1000,
    "quotes": {}
  }
]

$ curl localhost:8080/api/order -XPOST -H 'Content-type: application/json' \
  -d '{"username":"lbroudoux","orderType":"BUY","timestamp":1665130686122,"quote":"TYR","price":187.71,"number":1}' -s | jq
```


```shell
$ curl localhost:8083/api/order -XPOST -H 'Content-type: application/json' \
  -d '{"username":"lbroudoux","orderType":"BUY","timestamp":1665130686122,"quote":"TYR","price":187.71,"number":1}' -s | jq
```