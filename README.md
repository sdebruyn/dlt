# dlt-fabric

`dlt-fabric` is a maintained fork of [dlt](https://github.com/dlt-hub/dlt) with fixes for the Microsoft Fabric Warehouse destination and the related MS SQL family of destinations (mssql, synapse).

dlt's Fabric, mssql, and synapse destinations have open issues around authentication and reliability that are not yet released upstream. This fork carries the fixes on top of each dlt release so they can be used today, while the changes work their way through upstream review.

## What this fork carries

This fork applies four changes on top of the corresponding upstream dlt release:

- [dlt-hub/dlt#4140](https://github.com/dlt-hub/dlt/pull/4140): Microsoft Entra ID authentication for the mssql, synapse, and fabric destinations (service principal, managed identity, Azure CLI, interactive, and device code flows, in addition to plain SQL login).
- [dlt-hub/dlt#4141](https://github.com/dlt-hub/dlt/pull/4141): migration of the mssql, synapse, and fabric destinations from `pyodbc` to the `mssql-python` driver.
- [dlt-hub/dlt#4142](https://github.com/dlt-hub/dlt/pull/4142): a staging-optimized replace strategy for the Fabric destination, including a fix that makes concurrent multi-table-chain loads safe.
- [dlt-hub/dlt#4147](https://github.com/dlt-hub/dlt/pull/4147): support for an injectable pre-fetched `access_token` or an externally constructed `azure_credential` on the mssql, synapse, and fabric credentials, bypassing the usual `authentication` resolution.

These are proposed as pull requests against upstream dlt. Until they are merged and released, this fork is rebased onto each new dlt release to stay current.

## Installation

`dlt-fabric` is a drop-in replacement for `dlt`. Install it instead of the upstream package:

```bash
pip install dlt-fabric
# or
uv add dlt-fabric
```

Then use it exactly as you would use `dlt`:

```python
import dlt
```

Both packages install the same `dlt` import path, so `dlt-fabric` cannot be installed alongside the upstream `dlt` package in the same environment.

## Documentation

This fork does not maintain separate documentation. For everything beyond the fixes listed above, the upstream resources apply directly:

- Documentation and usage: https://dlthub.com/docs
- Upstream project: https://github.com/dlt-hub/dlt
