import pytest

from ingest_bench.specs.env import has_placeholder, resolve_env_placeholders


def test_has_placeholder_matches_only_the_documented_form() -> None:
    assert has_placeholder("${env:TOKEN}")
    assert has_placeholder("Bearer ${env:TOKEN}")
    assert not has_placeholder("literal")
    # Neither shell nor Kubernetes syntax is honoured: the point of one spelling
    # is that a value carrying any other one is a value, not a reference.
    assert not has_placeholder("${TOKEN}")
    assert not has_placeholder("$env:TOKEN")
    assert not has_placeholder("${env:1TOKEN}")


def test_resolve_replaces_every_occurrence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IB_TEST_USER", "svc")
    monkeypatch.setenv("IB_TEST_PASSWORD", "s3cret")
    assert resolve_env_placeholders(
        {
            "sasl.username": "${env:IB_TEST_USER}",
            "sasl.password": "${env:IB_TEST_PASSWORD}",
            "sasl.jaas.config": "user=${env:IB_TEST_USER} password=${env:IB_TEST_PASSWORD};",
            "security.protocol": "SASL_SSL",
        }
    ) == {
        "sasl.username": "svc",
        "sasl.password": "s3cret",
        "sasl.jaas.config": "user=svc password=s3cret;",
        "security.protocol": "SASL_SSL",
    }
    assert resolve_env_placeholders({}) == {}


def test_an_unset_variable_names_itself_and_the_property(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IB_TEST_ABSENT", raising=False)
    with pytest.raises(ValueError, match=r"\$\{env:IB_TEST_ABSENT\} is not set in the environment"):
        resolve_env_placeholders({"sasl.password": "${env:IB_TEST_ABSENT}"})
    with pytest.raises(ValueError, match="'sasl.password'"):
        resolve_env_placeholders({"sasl.password": "${env:IB_TEST_ABSENT}"})
