# Dev environment: metal-telemetry-operator on a local k3d cluster.
# Bring-up:  tilt up
# Tear-down: tilt down

allow_k8s_contexts("k3d-metal-telemetry-operator")

# Use the local registry created alongside the k3d cluster.
reg_port = "5001"
reg_host = "localhost:%s" % reg_port
def image_ref(name):
    return "%s/%s" % (reg_host, name)

# --- CRDs -----------------------------------------------------------------------
k8s_yaml("config/crd/bases/telemetry.metal.ironcore.dev_bmctelemetries.yaml")
k8s_resource(
    new_name = "crds",
    objects = [
        "bmctelemetries.telemetry.metal.ironcore.dev:customresourcedefinition",
    ],
)

# --- metal-telemetry-operator ---------------------------------------------------
arch = str(local("go env GOARCH", quiet=True)).strip()
docker_build(
    image_ref("metal-telemetry-operator"),
    ".",
    build_args = {"TARGETARCH": arch, "TARGETOS": "linux"},
)

k8s_yaml(helm(
    "./dist/chart",
    values = ["./dev/values.yaml"],
    set = [
        "manager.image.repository=" + image_ref("metal-telemetry-operator"),
        "manager.image.tag=latest",
    ],
))
k8s_resource("metal-telemetry-operator-controller-manager", resource_deps = ["crds"])
