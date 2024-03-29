#!/usr/bin/env python3
import argparse
import csv
import time
import logging
import sys
import os
from multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import requests

import tools.file_utils as util

log = logging.getLogger(__name__)

FILE_DIR = Path(__file__).parent.resolve()
ROOT_DIR = FILE_DIR.parent
APP_DIR = FILE_DIR.joinpath("applications")
TOOLS_DIR = FILE_DIR.joinpath("tools")
ISTIO_DIR = TOOLS_DIR.joinpath("istio-1.12.1")
ISTIO_BIN = ISTIO_DIR.joinpath("bin/istioctl")
YAML_DIR = FILE_DIR.joinpath("yaml_crds")
ISTIO_MANIFEST_DIR = APP_DIR.joinpath("microservices-demo/istio-manifests")

ONLINE_BOUTIQUE_DIR = APP_DIR.joinpath("microservices-demo")
TRAIN_TICKET_DIR = APP_DIR.joinpath(
    "train-ticket/deployment/kubernetes-manifests/k8s-with-istio")
SOCK_SHOP_DIR = APP_DIR.joinpath(
    "sock-shop/microservices-demo/deploy/kubernetes")

PROJECT_ID = "dynamic-tracing"
APPLY_CMD = "kubectl apply -f "
DELETE_CMD = "kubectl delete -f "
CLUSTER_CREATE = "gcloud container clusters create "

CONFIG_MATRIX = {
    'BK': {
        'minikube_startup_command': "minikube start --cpus=2 --memory 4096 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --num-nodes=5 ",
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {YAML_DIR}/bookinfo-services.yaml && \
                        {APPLY_CMD} {YAML_DIR}/bookinfo-apps.yaml && \
                        {APPLY_CMD} {ISTIO_DIR}/samples/bookinfo/networking/bookinfo-gateway.yaml && \
                        {APPLY_CMD} {ISTIO_DIR}/samples/bookinfo/networking/destination-rule-reviews.yaml && \
                        {APPLY_CMD} {YAML_DIR}/otelcollector_bookinfo_zipkin.yaml ",
        'undeploy_cmd': f"{ISTIO_DIR}/samples/bookinfo/platform/kube/cleanup.sh",
    },
    'OB': {
        'minikube_startup_command': "minikube start --cpus=6 --memory 8192 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --enable-autoscaling --min-nodes=5 --max-nodes=92 \
                                  --num-nodes=4  --machine-type e2-highmem-4 ", # to do experiments, 7 nodes
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {ONLINE_BOUTIQUE_DIR}/istio-manifests  && \
                        {APPLY_CMD} {ONLINE_BOUTIQUE_DIR}/kubernetes-manifests && \
                        {APPLY_CMD} {ONLINE_BOUTIQUE_DIR}/snicket_manifests  ",
        'undeploy_cmd': f"{DELETE_CMD} {ONLINE_BOUTIQUE_DIR}/istio_manifests && \
                          {DELETE_CMD} {ONLINE_BOUTIQUE_DIR}/kubernetes_manifests && \
                          {DELETE_CMD} {ONLINE_BOUTIQUE_DIR}/snicket_manifests  ",
    },
    'LG': {
        'minikube_startup_command': "minikube start --cpus=6 --memory 8192 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --enable-autoscaling --min-nodes=9 --max-nodes=14 \
                                  --num-nodes=12  --machine-type e2-highmem-8 ", # to do experiments, 7 nodes
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {APP_DIR}/load_generator/",
        'undeploy_cmd': f"{DELETE_CMD} {APP_DIR}/load_manifests "
    },
    'MB': {
        'minikube_startup_command': "minikube start --cpus=6 --memory 8192 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --enable-autoscaling --min-nodes=5 --max-nodes=6 \
                                  --num-nodes=5  --machine-type e2-highmem-8 ", # to do experiments, 7 nodes
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {APP_DIR}/microbricks/",
        'undeploy_cmd': f"{DELETE_CMD} {APP_DIR}/microbricks "
    },
    'LWE': {
        'minikube_startup_command': "minikube start --cpus=6 --memory 8192 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --enable-autoscaling --min-nodes=5 --max-nodes=92 \
                                  --num-nodes=4  --machine-type e2-highmem-4 ", # to do experiments, 7 nodes
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {APP_DIR}/load_w_elastic ",
        'undeploy_cmd': f"{DELETE_CMD} {APP_DIR}/load_w_elastic "
    },
    'LWT': {
        'minikube_startup_command': "minikube start --cpus=6 --memory 8192 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --enable-autoscaling --min-nodes=5 --max-nodes=92 \
                                  --num-nodes=5  --machine-type e2-highmem-8 ", # to do experiments, 7 nodes
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {APP_DIR}/load_w_tempo ",
        'undeploy_cmd': f"{DELETE_CMD} {APP_DIR}/load_w_tempo "
    },
    'TT': {
        'minikube_startup_command': None,
        'gcloud_flags': f" --enable-autoupgrade --num-nodes=5 ",
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; " +
                      f"{ISTIO_BIN} kube-inject -f {TRAIN_TICKET_DIR}/ts-deployment-part1.yml > dpl1.yml && " +
                      f"{APPLY_CMD} dpl1.yml && " +
                      f"{ISTIO_BIN} kube-inject -f {TRAIN_TICKET_DIR}/ts-deployment-part2.yml > dpl2.yml && " +
                      f"{APPLY_CMD} dpl2.yml && " +
                      f"{ISTIO_BIN} kube-inject -f {TRAIN_TICKET_DIR}/ts-deployment-part3.yml > dpl3.yml && " +
                      f"{APPLY_CMD} dpl3.yml && " +
                      f"{APPLY_CMD} {TRAIN_TICKET_DIR}/trainticket-gateway.yaml && " +
                      " rm dpl1.yml dpl2.yml dpl3.yml ",
        'undeploy_cmd': f"{DELETE_CMD} {TRAIN_TICKET_DIR}/ts-deployment-part1.yml && " +
                      f"{DELETE_CMD} {TRAIN_TICKET_DIR}/ts-deployment-part2.yml && " +
                      f"{DELETE_CMD} {TRAIN_TICKET_DIR}/ts-deployment-part3.yml "
    },
    'SS': {
        'minikube_startup_command': "minikube start --cpus=6 --memory 8192 --disk-size 32g",
        'gcloud_flags': f" --enable-autoupgrade --enable-autoscaling --min-nodes=5 --max-nodes=92 \
                                  --num-nodes=4  --machine-type e2-highmem-4 ",
        'deploy_cmd': f"kubectl create secret generic pubsub-key --from-file=key.json=service_account.json ; \
                        {APPLY_CMD} {SOCK_SHOP_DIR}/complete-demo.yaml && \
                        {APPLY_CMD} {YAML_DIR}/otelcollector_sock_shop_zipkin.yaml && \
                        {APPLY_CMD} {ISTIO_MANIFEST_DIR}/frontend-gateway.yaml ",
        'undeploy_cmd': f"{DELETE_CMD} {SOCK_SHOP_DIR}/complete-demo.yaml ",
    },
}

############## PLATFORM RELATED FUNCTIONS ###############################


def inject_istio(application):
    cmd = f"{ISTIO_BIN} install --set profile=demo "
    if application == "BK" or application == "TT" or application == "SS":
        cmd += "--set meshConfig.enableTracing=true --set meshConfig.defaultConfig.tracing.sampling=100 --set meshConfig.defaultConfig.tracing.zipkin.address=otelcollector:9411 --skip-confirmation "
    else:
        cmd += "--set meshConfig.enableTracing=true --set meshConfig.defaultConfig.tracing.sampling=100 --skip-confirmation "
    result = util.exec_process(cmd)
    if result != util.EXIT_SUCCESS:
        return result
    cmd = "kubectl label namespace default istio-injection=enabled --overwrite"
    result = util.exec_process(cmd)
    return result


def application_wait():
    cmd = "kubectl get deploy -o name"
    deployments = util.get_output_from_proc(cmd).decode("utf-8").strip()
    deployments = deployments.split("\n")
    for depl in deployments:
        wait_cmd = f"kubectl rollout status {depl} -w --timeout=180s"
        _ = util.exec_process(wait_cmd)
    log.info("Application is ready.")
    return util.EXIT_SUCCESS


def check_kubernetes_status():
    cmd = "kubectl cluster-info"
    result = util.exec_process(cmd,
                               stdout=util.subprocess.PIPE,
                               stderr=util.subprocess.PIPE)
    return result


def start_kubernetes(platform, multizonal, application, cluster_name):
    if platform == "GCP":
        cmd = CLUSTER_CREATE + cluster_name + CONFIG_MATRIX[application]['gcloud_flags']
        if multizonal:
            cmd += "--region us-central1-a --node-locations us-central1-b "
            cmd += "us-central1-c us-central1-a "
        else:
            cmd += "--zone=us-central1-a "
        result = util.exec_process(cmd)
        cmd = f"gcloud services enable container.googleapis.com --project {PROJECT_ID} && "
        cmd += f"gcloud services enable monitoring.googleapis.com cloudtrace.googleapis.com "
        cmd += f"clouddebugger.googleapis.com cloudprofiler.googleapis.com --project {PROJECT_ID}"
        result = util.exec_process(cmd)
        if result != util.EXIT_SUCCESS:
            return result

    else:
        if CONFIG_MATRIX[application]['minikube_startup_command'] != None:
            cmd = CONFIG_MATRIX[application]['minikube_startup_command']
            result = util.exec_process(cmd)
            if result != util.EXIT_SUCCESS:
                return result
        else:
            return "APPLICATION IS NOT SUPPORTED ON MINIKUBE"

    return result


def stop_kubernetes(platform, cluster_name):
    if platform == "GCP":
        cmd = f"gcloud container clusters delete "
        cmd += f"{cluster_name} --zone us-central1-a --quiet "
    else:
        # delete minikube
        cmd = "minikube delete"
    result = util.exec_process(cmd)
    return result


def get_gateway_info(platform):
    ingress_host = ""
    ingress_port = ""
    if platform == "GCP":
        cmd = "kubectl -n istio-system get service istio-ingressgateway "
        cmd += "-o jsonpath={.status.loadBalancer.ingress[0].ip} "
        ingress_host = util.get_output_from_proc(cmd).decode("utf-8").replace(
            "'", "")

        cmd = "kubectl -n istio-system get service istio-ingressgateway "
        cmd += " -o jsonpath={.spec.ports[?(@.name==\"http2\")].port}"
        ingress_port = util.get_output_from_proc(cmd).decode("utf-8").replace(
            "'", "")
    else:
        cmd = "minikube ip"
        ingress_host = util.get_output_from_proc(cmd).decode("utf-8").rstrip()
        cmd = "kubectl -n istio-system get service istio-ingressgateway"
        cmd += " -o jsonpath={.spec.ports[?(@.name==\"http2\")].nodePort}"
        ingress_port = util.get_output_from_proc(cmd).decode("utf-8")

    log.debug("Ingress Host: %s", ingress_host)
    log.debug("Ingress Port: %s", ingress_port)
    gateway_url = f"{ingress_host}:{ingress_port}"
    log.debug("Gateway: %s", gateway_url)

    return ingress_host, ingress_port, gateway_url


################### APPLICATION SPECIFIC FUNCTIONS ###########################

def get_deployments():
    cmd = "kubectl get deployments -o name "
    deployments = util.get_output_from_proc(cmd).decode("utf-8").strip()
    deployments = deployments.split("\n")
    return deployments

def autoscale(identifier, num_to_autoscale_by, deployments):
    print("deployments")
    for depl in deployments:
        if identifier in depl:
            cmd = f"kubectl autoscale {depl} --min=" + num_to_autoscale_by + " --max=" + num_to_autoscale_by + " --cpu-percent=70"
            result = util.exec_process(cmd)
            if result != util.EXIT_SUCCESS:
                return result
    # autoscaler kicks in every 15 seconds
    time.sleep(16)
    application_wait()

def deploy_application(application, cluster_name, tracegen_autoscaling, backend_autoscaling):
    if check_kubernetes_status() != util.EXIT_SUCCESS:
        log.error("Kubernetes is not set up."
                  " Did you run the deployment script?")
        sys.exit(util.EXIT_FAILURE)
    # if we are load generator, deploy the collector in two parts:
    if application == 'LG' or application == 'MB':
        cmd = CONFIG_MATRIX[application]['deploy_cmd']
        result = util.exec_process(cmd + "jaeger.yaml")
        application_wait()
        result = util.exec_process(cmd + "otelcollectorbackend.yaml")
        application_wait()
        autoscale("otelcollectorbackend", backend_autoscaling, get_deployments())
        result = util.exec_process(cmd + "otelcollector.yaml")
        application_wait()
        result = util.exec_process(cmd + "tracegen.yaml")
        application_wait()
        autoscale("tracegen", tracegen_autoscaling, get_deployments())
    else:
        cmd = CONFIG_MATRIX[application]['deploy_cmd']
        result = util.exec_process(cmd)
        application_wait()

    # Now do autoscaling
    log.info("Starting horizontal autoscaling")
    for depl in get_deployments():
        # Sometimes, the list contains whitespace.
        if not depl.strip():
            continue
        if "otelcollector" in depl or "tracegen" in depl:
            continue # already autoscaled above
        else:
            cmd = f"kubectl autoscale {depl} --min=1 --max=10 --cpu-percent=40"
        result = util.exec_process(cmd)
        if result != util.EXIT_SUCCESS:
            return result
    application_wait()

    return result

def load_test(num_tracegens):
    if check_kubernetes_status() != util.EXIT_SUCCESS:
        log.error("Kubernetes is not set up."
                  " Did you run the deployment script?")
        sys.exit(util.EXIT_FAILURE)
    cmd = "kubectl get deployments -o name "
    deployments = util.get_output_from_proc(cmd).decode("utf-8").strip()
    deployments = deployments.split("\n")
    log.info("Starting load test")
    for depl in deployments:
        if "tracegen" in depl:
            cmd = f"kubectl autoscale {depl} --min={num_tracegens} --max={num_tracegens} --cpu-percent=40"
            result = util.exec_process(cmd)
            if result != util.EXIT_SUCCESS:
                return result
    application_wait()
    return result

def remove_application(application):
    cmd = CONFIG_MATRIX[application]['undeploy_cmd']
    cmd += f"{DELETE_CMD} {YAML_DIR}/root-cluster.yaml "
    result = util.exec_process(cmd)
    return result


def setup_application_deployment(platform, multizonal, application, cluster_name, tracegen_autoscaling, backend_autoscaling):
    result = start_kubernetes(platform, multizonal, application, cluster_name)
    if result != util.EXIT_SUCCESS:
        return result
    result = deploy_application(application, cluster_name, tracegen_autoscaling, backend_autoscaling)
    if result != util.EXIT_SUCCESS:
        return result
    return result

def count_traces():
    cmd = "kubectl get pods -o name"
    tracegens = util.get_output_from_proc(cmd).decode("utf-8").strip()
    tracegens = tracegens.split("\n")
    total_traces = 0
    for pod in tracegens:
        if "tracegen" not in pod:
            continue
        cmd = "kubectl logs " + pod + " --tail 1"
        log_line = util.get_output_from_proc(cmd).decode("utf-8").strip()
        if "traces generated" not in log_line:
            print("Traces either haven't finished being generated, or something has gone wrong. see pod ", pod)
        beginning_of_num = log_line.rfind(':')
        num_traces = int(log_line[beginning_of_num+1:len(log_line)-1])
        total_traces = num_traces + total_traces
    print("total traces were: ", total_traces)
    return 0

def main(args):
    # single commands to execute
    if args.setup:
        return setup_application_deployment(args.platform, args.multizonal, args.application, args.cluster_name, args.tracegen_autoscaling, args.backend_autoscaling)
    if args.deploy_application:
        return deploy_application(args.application, args.cluster_name, args.tracegen_autoscaling, args.backend_autoscaling)
    if args.remove_application:
        return remove_application(args.application)
    if args.clean:
        return stop_kubernetes(args.platform, args.cluster_name)
    if args.count_traces:
        return count_traces()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-l",
                        "--log-file",
                        dest="log_file",
                        default="run_microservices.log",
                        help="Specifies name of the log file.")
    parser.add_argument(
        "-ll",
        "--log-level",
        dest="log_level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
        help="The log level to choose.")
    parser.add_argument("-p",
                        "--platform",
                        dest="platform",
                        default="KB",
                        choices=["MK", "GCP"],
                        help="Which platform to run the scripts on."
                        "MK is minikube, GCP is Google Cloud Platform")
    parser.add_argument("-a",
                        "--application",
                        dest="application",
                        default="BK",
                        choices=["BK", "OB", "LG", "SS", "TT", "LWE", "LWT", "MB"],
                        help="Which application to deploy."
                        "BK is Bookinfo, OB is Online Boutique, LG is artificial load generator, SS is sock shop, and TT is Train Ticket")
    parser.add_argument("-cn",
                        "--cluster-name",
                        dest="cluster_name",
                        default="demo",
                        help="Name of the GCP cluster to create")
    parser.add_argument("-m",
                        "--multi-zonal",
                        dest="multizonal",
                        action="store_true",
                        help="If you are running on GCP,"
                        " do you want a multi-zone cluster?")
    parser.add_argument("-s",
                        "--setup",
                        dest="setup",
                        action="store_true",
                        help="Just do a deployment. "
                        "This means installing the application and Kubernetes."
                        " Do not run any experiments.")
    parser.add_argument("-c",
                        "--clean",
                        dest="clean",
                        action="store_true",
                        help="Clean up an existing deployment. ")
    parser.add_argument("-ta",
                        "--tracegen-autoscaling",
                        dest="tracegen_autoscaling",
                        default="1",
                        help="do load testing with this many thousand traces per second")
    parser.add_argument("-ba",
                        "--backend-autoscaling",
                        dest="backend_autoscaling",
                        default="1",
                        help="create this many backends")
    parser.add_argument("-db",
                        "--deploy-application",
                        dest="deploy_application",
                        action="store_true",
                        help="Deploy the app. ")
    parser.add_argument("-rb",
                        "--remove-application",
                        dest="remove_application",
                        action="store_true",
                        help="remove the app. ")
    parser.add_argument("-ct",
                        "--count-traces",
                        dest="count_traces",
                        action="store_true",
                        help="count number of generated traces. ")
    # Parse options and process argv
    arguments = parser.parse_args()
    # configure logging
    logging.basicConfig(filename=arguments.log_file,
                        format="%(levelname)s:%(message)s",
                        level=getattr(logging, arguments.log_level),
                        filemode="w")
    stderr_log = logging.StreamHandler()
    stderr_log.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
    logging.getLogger().addHandler(stderr_log)
    sys.exit(main(arguments))

