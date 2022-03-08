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

### Other Notes
The version of Online Boutique in this repository is found here:  https://github.com/julianocosta89/opentelemetry-microservices-demo.  It was edited only to allow it to run the kubernetes manifests independently through giving it absolute image names instead of relative.
