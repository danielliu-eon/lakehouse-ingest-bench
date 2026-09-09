"""Kafka client properties, and the one authentication a file cannot hold.

Everything a site declares under `kafka.security` reaches a client verbatim,
which is what lets a cluster this repository has never heard of be reached by
configuration alone. Amazon MSK's IAM authentication is the exception, and not
by choice: SASL/OAUTHBEARER against it wants a token signed from the caller's
own credentials, per connection, that expires within the hour. No property can
carry one. What a site can carry is the region to sign in, which it declares as
the pseudo-key `aws.region` — the harness's own, and removed here, because
librdkafka refuses a configuration property it does not recognise.

A site that would rather arrange its own tokens says so by setting any
`sasl.oauthbearer.*` property, and this module leaves its configuration alone.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

# The region to sign an MSK token in, and the only key this module removes.
REGION_KEY = "aws.region"

_MECHANISM_KEY = "sasl.mechanism"
_OAUTHBEARER = "OAUTHBEARER"
_OAUTHBEARER_PREFIX = "sasl.oauthbearer."

# A region, to a signed token and the moment it expires in milliseconds since
# the epoch — the units the MSK signer reports.
TokenProvider = Callable[[str], tuple[str, int]]

# librdkafka hands the callback the value of `sasl.oauthbearer.config`, which is
# None when no such property is set, and reads the expiry back in seconds.
OauthCallback = Callable[[str | None], tuple[str, float]]


def msk_token_provider() -> TokenProvider:
    """The Amazon MSK IAM token signer, imported at the moment one is needed.

    The import is here rather than at module scope because the signer is an
    optional extra: the harness has to be importable, and every site that is not
    on MSK runnable, on a machine that has never installed an AWS SDK.
    """
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as error:
        raise ValueError(
            f"{_MECHANISM_KEY}={_OAUTHBEARER} signs an Amazon MSK token, and the signer is not installed; "
            "install this harness with its 'aws' extra"
        ) from error

    def sign(region: str) -> tuple[str, int]:
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms

    return sign


def _oauth_callback(region: str, token_provider: TokenProvider, on_token: Callable[[], None] | None) -> OauthCallback:
    def callback(_oauthbearer_config: str | None) -> tuple[str, float]:
        token, expiry_ms = token_provider(region)
        if on_token is not None:
            on_token()
        # Two libraries, two units. A token whose expiry is read as seconds
        # when it was reported in milliseconds is refused as long expired.
        return token, expiry_ms / 1000

    return callback


def librdkafka_config(
    security: Mapping[str, object],
    *,
    token_provider: TokenProvider | None = None,
    on_token: Callable[[], None] | None = None,
) -> dict[str, object]:
    """``security`` as a Kafka client takes it, with an MSK token callback if it needs one.

    ``on_token`` is called every time librdkafka asks for a token, so a caller
    that has to drive the callback itself can tell when it has run.
    """
    config = {key: value for key, value in security.items() if key != REGION_KEY}
    if _MECHANISM_KEY not in security or security[_MECHANISM_KEY] != _OAUTHBEARER:
        return config
    # The site arranges its own tokens — librdkafka's OIDC path, or a token it
    # holds. A second source of them would override whatever it set up.
    if any(key.startswith(_OAUTHBEARER_PREFIX) for key in security):
        return config
    if REGION_KEY not in security:
        raise ValueError(
            f"{_MECHANISM_KEY}={_OAUTHBEARER} signs a token per connection, so the client properties must also "
            f"set {REGION_KEY!r}: the region to sign it in"
        )
    region = security[REGION_KEY]
    if not isinstance(region, str):
        raise ValueError(f"{REGION_KEY!r} must be a region name, got {region!r}")
    provider = msk_token_provider() if token_provider is None else token_provider
    config["oauth_cb"] = _oauth_callback(region, provider, on_token)
    return config
