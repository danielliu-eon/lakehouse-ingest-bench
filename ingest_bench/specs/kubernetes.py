"""The Kubernetes shape of a managed engine, for the drivers that address it.

A run on a cluster is one custom resource, and what a driver does to it is the
same sequence whatever engine it is: apply the two documents, wait for the
resource to report itself running, tail its log if it does not, tunnel to the
HTTP API it publishes, read back the image its pod pulled, delete both
documents. Only the names differ — the resource's kind, where its state sits
in the status, what "running" is called there, which Service carries the API.

So each engine declares those names once and the drivers read them. That is
what keeps a third engine out of the shell: adding one is a descriptor and an
``engines/<name>/`` package, not another branch in ``stage.sh``.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

# Where a run's object name goes in the strings that need one. Substituted by
# the caller, because the driver is what holds the name and a descriptor is a
# constant.
NAME = "<name>"


@dataclass(frozen=True)
class EngineKubernetes:
    """How one managed engine's run is addressed on a cluster.

    ``log_target``, ``provenance_selector`` and ``pods_selector`` carry `NAME`
    wherever the run's object name belongs. ``pods_selector`` is empty for an
    engine whose check reads nothing off the pods, and its check is then not
    given a pod list at all.
    """

    kind: str
    running_state: str
    failed_states: tuple[str, ...]
    state_jsonpath: str
    rest_service_suffix: str
    rest_port: int
    log_target: str
    provenance_selector: str
    pods_selector: str
    document_file: str
    configmap_file: str

    def texts(self) -> dict[str, str]:
        """Every field as the one line of text a shell driver reads it as.

        The list of states is comma-joined and the port is decimal, because a
        driver matches a state against that list and hands the port to
        `kubectl`: both are strings by the time they leave here, so nothing in
        the shell has to know which field was which type.
        """
        return {
            "kind": self.kind,
            "running_state": self.running_state,
            "failed_states": ",".join(self.failed_states),
            "state_jsonpath": self.state_jsonpath,
            "rest_service_suffix": self.rest_service_suffix,
            "rest_port": str(self.rest_port),
            "log_target": self.log_target,
            "provenance_selector": self.provenance_selector,
            "pods_selector": self.pods_selector,
            "document_file": self.document_file,
            "configmap_file": self.configmap_file,
        }


# The field names, in declaration order, which is the order `engine-k8s` prints
# them in. Read off the dataclass so that a field added without a line in
# `texts` is a failure rather than a field no driver can ask for.
FIELDS: tuple[str, ...] = tuple(field.name for field in fields(EngineKubernetes))


def for_name(text: str, name: str) -> str:
    """``text`` with `NAME` replaced by a run's object name."""
    return text.replace(NAME, name)
