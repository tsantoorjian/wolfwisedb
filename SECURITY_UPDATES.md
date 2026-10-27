# Python dependency security update — 2026-10-26

Use Python 3.12 and install the pinned dependencies with:

```sh
python -m pip install --upgrade pip==26.2.2
python -m pip install -r requirements.txt
python -m pip check
```

`certifi==2026.10.19` removes the compromised emSign ECC Root CA - G3 trust
anchor addressed by [GHSA-6q48-6c7w-9gw2](https://github.com/advisories/GHSA-6q48-6c7w-9gw2).
pip 26.2.2 includes the same repaired certifi bundle in its vendored copy.
`aiohttp==3.14.4` addresses the open aiohttp advisories as of this update;
aiohappyeyeballs 2.5.0, aiosignal 1.4.0, and yarl 1.25.1 meet its minimum
dependency requirements. aiohttp and yarl now require Python 3.10 or later,
so the workflow's Python version advances from 3.9 to 3.12.

Validation used a fresh Python 3.12 environment: the complete requirements
installed, `pip check` passed, the legacy client imports passed, and both
requests and aiohttp completed verified HTTPS requests with the repaired
CA bundle. The standalone and pip-vendored bundles were byte-identical and
did not contain the compromised root. No NBA API requests or ETL writes were
performed.

The GitHub Actions workflow was disabled for inactivity before this update.
This change preserves that state and does not run or re-enable the historical
ETL. Existing environments need dependency reinstallation; any downstream
container image must be rebuilt before it receives the fix.

Other packages have separate Dependabot alerts. This targeted change resolves
certifi and aiohttp; it does not claim that all dependencies are vulnerability
free.
