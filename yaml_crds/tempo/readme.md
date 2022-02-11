## Tempo

First of all, you need to download the access key for ```snickey``` service account and replace the currently empty file ```snickey-key.json``` with the downloaded key. Keep the filename same as ```snickey-key.json```.

Run: 
```console
docker-compose up -d
```

Multiple containers will spin up. A load generator will start generating load that will be directed to an otel collector. The otel collector will send the traces to tempo which will offload them to a GCS bucket. 

For stopping all the containers, run:
```console
docker-compose down -v
```
