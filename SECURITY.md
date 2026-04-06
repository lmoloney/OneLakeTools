# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅ Current |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do not** open a public issue.
2. Email the maintainer via the address listed on their [GitHub profile](https://github.com/lmoloney).
3. Include a description of the vulnerability, steps to reproduce, and potential impact.

You should receive an acknowledgement within 48 hours. The maintainer will work with you to understand the issue, determine its severity, and coordinate a fix before any public disclosure.

## Scope

This project authenticates to Microsoft Fabric using `DefaultAzureCredential`. It does **not** store credentials, tokens, or secrets. Authentication tokens are held in memory only for the duration of the session.

Areas of particular security interest:

- Token handling in `onelake_client/auth.py`
- HTTP request construction in `onelake_client/_http.py`
- Any user-supplied input rendered via Rich markup (see `rich.markup.escape()` usage)
