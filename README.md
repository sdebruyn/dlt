# dlt-fabric

`dlt-fabric` is a maintained fork of [dlt](https://github.com/dlt-hub/dlt) with fixes for the Microsoft Fabric Warehouse destination and the related MS SQL family of destinations (mssql, synapse).

dlt's Fabric, mssql, and synapse destinations have open issues around authentication and reliability that are not yet released upstream. This fork carries the fixes on top of each dlt release so they can be used today, while the changes work their way through upstream review.

## What this fork carries

This fork applies the following changes on top of the corresponding upstream dlt release:

- [dlt-hub/dlt#4147](https://github.com/dlt-hub/dlt/pull/4147): Microsoft Entra ID authentication for the mssql, synapse, and fabric destinations, an injectable pre-fetched `access_token` or externally constructed `azure_credential`, and `authentication = "fab_notebookutils"` for pipelines running inside a Fabric notebook. Supersedes the closed [#4140](https://github.com/dlt-hub/dlt/pull/4140).
- [dlt-hub/dlt#4141](https://github.com/dlt-hub/dlt/pull/4141): migration of the mssql, synapse, and fabric destinations from `pyodbc` to the `mssql-python` driver, which bundles its own client libraries so no system ODBC install is needed.
- [dlt-hub/dlt#4142](https://github.com/dlt-hub/dlt/pull/4142): a staging-optimized replace strategy for the Fabric destination, including a fix that makes concurrent multi-table-chain loads safe.
- [dlt-hub/dlt#4258](https://github.com/dlt-hub/dlt/pull/4258): a `mssql_arrow` backend for `sql_database` that uses `mssql-python`'s native Arrow C Data Interface for zero-copy batch extraction.
- [dlt-hub/dlt#4259](https://github.com/dlt-hub/dlt/pull/4259): correct nvarchar-to-varchar length scaling in the Fabric type mapper (character precision x 4 for UTF-8 byte semantics).
- [dlt-hub/dlt#4260](https://github.com/dlt-hub/dlt/pull/4260): allow `time` columns through the Parquet load path on Fabric (the inherited Synapse rejection does not apply).
- [dlt-hub/dlt#4261](https://github.com/dlt-hub/dlt/pull/4261): map SQL Server `MONEY` and `SMALLMONEY` types to `decimal(19,4)` and `decimal(10,4)` in `sql_database` schema inference.
- [dlt-hub/dlt#4275](https://github.com/dlt-hub/dlt/pull/4275): an Azure Key Vault configuration provider for loading secrets and config from Azure Key Vault, with `DefaultAzureCredential` fallback and a `dlt[azure_key_vault]` extra.
- [dlt-hub/dlt#4302](https://github.com/dlt-hub/dlt/pull/4302): route OneLake filesystem configurations to a dedicated client that strips trailing separators before directory probes, which OneLake answers with `403 AuthenticationFailed`.
- [dlt-hub/dlt#4307](https://github.com/dlt-hub/dlt/pull/4307): widen `tinyint` to `smallint` in the Fabric type mapper, since Fabric Warehouse has no `tinyint` and rejects the inherited SQL Server mapping.
- [dlt-hub/dlt#4357](https://github.com/dlt-hub/dlt/pull/4357): load mssql Parquet files through `mssql-python`'s native Arrow bulk copy instead of ADBC, which drops the second driver stack and makes Parquet work under Entra ID token authentication.

These are proposed as pull requests against upstream dlt. Until they are merged and released, this fork is rebased onto each new dlt release to stay current.

## How the branches fit together

Everything is developed as small branches that each become one upstream pull request. Some of them
are stacked, because their content genuinely depends on another PR; the rest sit directly on
`devel`.

```
upstream/devel
├── feat/mssql-access-token-credential            #4147  Entra ID auth, access_token/azure_credential, fab_notebookutils
│   ├── feat/azure-key-vault-provider             #4275  AzureKeyVaultProvider (uses the NotebookUtils credential)
│   └── feat/mssql-python-driver                  #4141  pyodbc -> mssql-python migration
│       ├── feat/2-mssql-python-arrow-batches     #4258  mssql_arrow extraction backend
│       └── feat/13-mssql-python-arrow-bulk-copy  #4357  native Arrow bulk copy for parquet load jobs
├── feat/fabric-staging-optimized                 #4142  staging-optimized replace via DDL transactions
├── fix/3-nvarchar-utf8-length                    #4259
├── fix/4-fabric-time-parquet                     #4260
├── fix/5-money-decimal-precision                 #4261
├── fix/8-onelake-directory-probes                #4302
└── fix/4306-fabric-tinyint-smallint              #4307
```

`#4141` is stacked on `#4147` rather than branched from `devel` so the Entra ID authentication
exists once. When it was branched independently, both carried their own copy of the same two auth
commits, which meant every change had to be made twice and the integration branch inherited the
duplication.

`#4258` and `#4357` are siblings on top of `#4141`, not a chain. One adds an Arrow *extraction*
backend to `sql_database`; the other replaces the mssql *destination*'s Parquet load job. Both need
the `mssql-python` driver and neither needs anything from the other, so either can land first and
neither is rebased onto the other. `#4357` is on `#4141` rather than on `devel` because
`Cursor.bulkcopy_arrow()` is an mssql-python API with no pyODBC equivalent.

GitHub cannot express this as a real stacked PR: a pull request's base must live in the upstream
repository, and these branches live in the fork. Each PR body therefore names the PR it depends on,
and the diff shown on GitHub includes the parent's commits until the parent lands.

## Rebuilding the integration and release branches

`integrated-all-prs` is every PR merged together, for testing the combination. It **starts from
`devel`**. Rebuild it with plain merges; the stacking means the two leaves carry everything below
them:

```sh
git checkout integrated-all-prs && git reset --hard devel
git merge --no-ff feat/2-mssql-python-arrow-batches     # brings #4147 and #4141 along
git merge --no-ff feat/13-mssql-python-arrow-bulk-copy  # shares #4141, so only the bulk copy is new
git merge --no-ff feat/azure-key-vault-provider         # shares #4147, so this merges cleanly
git merge --no-ff feat/fabric-staging-optimized
git merge --no-ff fix/3-nvarchar-utf8-length
git merge --no-ff fix/4-fabric-time-parquet
git merge --no-ff fix/5-money-decimal-precision
git merge --no-ff fix/8-onelake-directory-probes
git merge --no-ff fix/4306-fabric-tinyint-smallint
```

Conflicts here are almost always two branches appending tests to the same file; keep both sides.

`dlt-fabric` is the published branch and **starts from `upstream/master`**, the latest stable
release. Do not merge `integrated-all-prs` into it: that branch sits on `devel`, so merging it
would drag every unreleased `devel` commit into the release. Cherry-pick the feature commits
instead, which is exactly the set that is in the integration branch but not in `devel`:

```sh
git checkout dlt-fabric && git reset --hard upstream/master
git rev-list --reverse --no-merges devel..integrated-all-prs | while read c; do git cherry-pick "$c"; done
git cherry-pick <package as dlt-fabric> <resolve version and package name>
```

Verify afterwards that nothing from `devel` slipped in:

```sh
git rev-list upstream/master..dlt-fabric | while read c; do
  git merge-base --is-ancestor "$c" devel && git log -1 --oneline "$c"
done   # must print nothing
```

## Versioning and release

`pyproject.toml` sets `name = "dlt-fabric"` and the version of the upstream release this fork sits
on, with a `.postN` suffix per fork release. `dlt/version.py` resolves the distribution name so
`dlt.__version__` keeps working under the renamed package. Publish with `make publish-library`.

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
