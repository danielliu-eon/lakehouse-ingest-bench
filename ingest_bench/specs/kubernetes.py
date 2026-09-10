# SPDX-License-Identifier: Apache-2.0
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

    ``error_jsonpath`` is where the operator writes what went wrong, and
    ``lifecycle_jsonpath`` is what says whether it has given up. A document
    rejected outright never gains a state — the state belongs to a job the
    operator did not create — so the error field is the only thing that
    distinguishes a rejection from a run still being placed; and an error
    alone does not distinguish them, because a reconcile the operator will
    retry writes one too. The lifecycle is matched against `failed_states`,
    the same set as the state, since an operator that reports a run's own
    failure and one that reports the document's use the same words for it.
    For an engine whose application state *is* its lifecycle, the two paths
    are the same path.

    ``max_object_name_length`` is the longest object name the engine's operator
    accepts, and ``None`` for one that publishes no bound of its own. Staging
    checks the derived name against it, so it is the one field below that no
    driver reads.
    """

    kind: str
    running_state: str
    failed_states: tuple[str, ...]
    state_jsonpath: str
    error_jsonpath: str
    lifecycle_jsonpath: str
    rest_service_suffix: str
    rest_port: int
    log_target: str
    provenance_selector: str
    pods_selector: str
    document_file: str
    configmap_file: str
    max_object_name_length: int | None = None

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
            "error_jsonpath": self.error_jsonpath,
            "lifecycle_jsonpath": self.lifecycle_jsonpath,
            "rest_service_suffix": self.rest_service_suffix,
            "rest_port": str(self.rest_port),
            "log_target": self.log_target,
            "provenance_selector": self.provenance_selector,
            "pods_selector": self.pods_selector,
            "document_file": self.document_file,
            "configmap_file": self.configmap_file,
        }


# The fields no shell driver reads. A length is not a name a driver addresses:
# it is checked in Python, before the run has an object to address. Named here
# rather than quietly left out of `texts`, so that FIELDS below still makes a
# field added without a line there a failure and not a field no driver can ask
# for.
_UNPRINTED = frozenset({"max_object_name_length"})

# The field names a driver reads, in declaration order, which is the order
# `engine-k8s` prints them in.
FIELDS: tuple[str, ...] = tuple(field.name for field in fields(EngineKubernetes) if field.name not in _UNPRINTED)


def for_name(text: str, name: str) -> str:
    """``text`` with `NAME` replaced by a run's object name."""
    return text.replace(NAME, name)


def object_name(run_id: str) -> str:
    """``run_id`` as a Kubernetes object name.

    An RFC 1123 name is lowercase and a run id's stamp is not: the `T` and the
    `Z` in it are refused by the API server. Only the object names are
    lowercased — the run id itself is the identifier the topic, the table and
    the run directory are addressed by, and it stays as it is. The shell says
    the same thing in `k8s_object_name`, because the drivers address the
    objects the renderers named.
    """
    return run_id.lower()
