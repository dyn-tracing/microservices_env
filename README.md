# microservices_env
This runs various microservice applications so that one can extract traces.


## Setup
`tools/setup.sh` should install most if not all of the dependencies necessary to run everything.

## To compile the collector
The collector's code all lives within custom_opentelemetry_collector.  To build, run `docker build .` in the root of this directory.  Then you can push that image and use it in a cluster.

## To run the application with the collector
If you have a custom-built collector, change the image to the one you created in 
microservices_env/applications/opentelemetry-microservices-demo/kubernetes-manifests/otelcollector.yaml.

To run the collector so that it can access Google Storage, it needs a service account.  Make a service account with Google Storage permissions and store the json key with the name service_account.json in the root of this repository.  NEVER COMMIT THIS KEY ANYWHERE PUBLIC, INCLUDING THIS REPOSITORY.

Then, to run an application with the collector, simply do 
```
python3 run_microservices.py -a OB -s
```
By default, this runs on Minikube.  To run on Google Cloud Platform, run with the flag `-p GCP`. 

The collector by default stores traces within Cloud Storage.

## Commands to run the application with the collector on GCP
The following commands are run under the folder /microservices_env.

First, to deploy the application, do
```
python3 run_microservices.py -p GCP -s
```
Then, check the status of the pods and wait until all pods are running by doing
```
kubectl get pods
```
Note: Record the name of the pod with the format "otelcollector-<some_hash>" to check the otelcollector logs in the later steps.

To determine the ingress ip and ports, run the following commands
```
export INGRESS_HOST=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
export INGRESS_PORT=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{.spec.ports[?(@.name=="http2")].port}')
export SECURE_INGRESS_PORT=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{.spec.ports[?(@.name=="https")].port}')
export TCP_INGRESS_PORT=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{.spec.ports[?(@.name=="tcp")].port}')
```
Now, check the ingress IP and ports by
```
echo $INGRESS_HOST $INGRESS_PORT
```
At this step, you are all set to visit the product page of Bookinfo by going to $INGRESS_HOST:$INGRESS_PORT/productpage on your browser. The two variables are obtained through the previous step.

After visiting the page, check the otelcollector log by running
```
kubectl logs otelcollector-<some_hash>
```
If you see output from the logging exporter, then the collector is receiving traces.

Note: The otelcollector-<some_hash> is the pod name with the format "otelcollector-<some_hash>" that you recorded from the output of "kubectl get pods".

Finally, to clear the cluster, simply do
```
python3 run_microservices.py -p GCP -c
```

### Other Notes
The version of Online Boutique in this repository is found here:  https://github.com/julianocosta89/opentelemetry-microservices-demo.  It was edited only to allow it to run the kubernetes manifests independently through giving it absolute image names instead of relative.


### Commands to run the train-ticket application with the collector on GCP

Start the train-ticket application using the follow command in the microservices_env directory
```
python3 run_microservices.py -a TT -p GCP -s
```
Then go to yaml_crds and apply the yaml file for otelcollector using
```
kubectl apply -f otelcollector_train_ticket_zipkin.yaml
```
Wait for a few minutes and check if all the pods are running using
```
kubectl get pods
```
Make sure the otelcollector-<some hash> and the ts-ui-dashboard-<some hash> are running.

Then go to Google Cloud Platform > Navigation Menu > More Products > Kubernetes Engine > Services & Ingress. In the drop-down menu, choose the name of your cluster, click ok

In the filter type in "istio-ingressgateway", find the row with type called "External load balancer", expand the row ( using the ">" on the end of the row besides the name of the cluster). Click <strong>[IP]:80</strong> to visit the train-ticket web page. 

Click around and perform some action (i.e. login, search, etc.) 

Go back to GCP console, and check the logs for the otelcollector-<some hash> using 
```
kubectl logs otelcollector-<some_hash>
```
Clear the cluster using
```
python3 run_microservices.py -a TT -p GCP -c
```

