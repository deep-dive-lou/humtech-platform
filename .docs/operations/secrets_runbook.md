# Operations Runbook: Secrets & Tenant Credential Encryption

## Purpose
Define how HumTech manages secrets securely in production, without storing sensitive keys in the database.

This document is for:
- onboarding new tenants
- rotating secrets safely
- debugging auth/decryption issues without panic

---

## Terminology

### KEK (Key Encryption Key)
- Also called the **master key**
- Stored outside the database
- Provided to the app via environment variable
- Used only to unwrap tenant keys

Env var:
- `CREDENTIALS_KEK` (preferred)
- Legacy alias: `TENANT_ENCRYPTION_KEY`

### DEK (Data Encryption Key)
- One per tenant
- Random Fernet key
- Stored **encrypted** (wrapped) in the database
- Used to encrypt tenant credentials

---

## What lives where

### Outside the DB (server secret)
- `CREDENTIALS_KEK`
- Stored at: `/etc/humtech/humtech.env`
- Permissions: root-only (600)

### Inside the DB (safe to store)
- `wrapped_dek` (tenant DEK encrypted with KEK)
- `credentials_enc` (credentials encrypted with tenant DEK)

If the DB leaks without the KEK, credentials are unreadable.

---

## Production Setup (current)

### Secret storage
- Master key stored on droplet:
  - `/etc/humtech/humtech.env`
- Injected into containers via docker-compose:
  - `env_file: /etc/humtech/humtech.env`

### Containers that require the KEK
- `humtech_api`
- `humtech_runner`

Both must receive the same KEK.

---

## Onboarding a new tenant (future flow)

1. Generate a random DEK
2. Wrap DEK using KEK → `wrapped_dek`
3. Encrypt credentials using DEK → `credentials_enc`
4. Store both in DB
5. No new env vars required

---

## Rotation strategy (future)

### Rotate tenant DEK
- Decrypt creds with old DEK
- Generate new DEK
- Re-encrypt creds
- Update wrapped_dek
- No impact on other tenants

### Rotate KEK (advanced)
- Unwrap each tenant DEK with old KEK
- Re-wrap with new KEK
- No need to re-encrypt credentials
- Requires maintenance window

---

## Failure modes & diagnosis

### Symptom: calendar auth_error
Check:
1. Is `CREDENTIALS_KEK` present in container env?
2. Was the KEK the same one used to encrypt creds?
3. Does tenant have wrapped_dek / credentials_enc?

### Symptom: works in API but not runner
- Runner container missing KEK env
- Restart runner after secret changes

---

## Security rules (non-negotiable)
- Never store KEK in Postgres
- Never log decrypted credentials
- Never commit secrets to git
- Fail loudly if KEK is missing

---

## Safe stopping point
If:
- KEK exists on server
- containers load it
- no tenants are migrated yet

System is secure and ready for next phase.