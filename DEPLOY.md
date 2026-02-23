# Deploying humtech-platform

## One-command deploy (from local machine)

```bash
# 1. Commit and push your changes
git add <files> && git commit -m "..." && git push origin main

# 2. SSH into droplet, pull, rebuild, restart
ssh -i C:/Users/loumk/.ssh/id_ed25519 root@144.126.225.168 \
  'cd /opt/humtech/app && git pull && docker compose -f docker-compose.prod.yml build --no-cache && docker compose -f docker-compose.prod.yml up -d'
```

## Via PowerShell (Claude Code workaround — captures SSH output)

```powershell
powershell -Command "ssh -i C:/Users/loumk/.ssh/id_ed25519 -o StrictHostKeyChecking=no root@144.126.225.168 'cd /opt/humtech/app && git pull && docker compose -f docker-compose.prod.yml build --no-cache && docker compose -f docker-compose.prod.yml up -d 2>&1 | tail -5' | Out-File C:/Users/loumk/ssh_out.txt -Encoding utf8; Get-Content C:/Users/loumk/ssh_out.txt"
```

## Key details

- Droplet IP: `144.126.225.168`
- SSH key: `C:\Users\loumk\.ssh\id_ed25519`
- App lives at: `/opt/humtech/app/` on the droplet
- Containers: `app-humtech_api-1` (FastAPI) and `app-humtech_runner-1` (worker)
- **Always rebuild both containers** — they share the same Dockerfile/code

## Check logs

```powershell
# Runner logs (bot processing)
powershell -Command "ssh -i C:/Users/loumk/.ssh/id_ed25519 -o StrictHostKeyChecking=no root@144.126.225.168 'docker logs app-humtech_runner-1 --tail 50 2>&1' | Out-File C:/Users/loumk/ssh_out.txt -Encoding utf8; Get-Content C:/Users/loumk/ssh_out.txt"

# API logs (webhook / HTTP)
powershell -Command "ssh -i C:/Users/loumk/.ssh/id_ed25519 -o StrictHostKeyChecking=no root@144.126.225.168 'docker logs app-humtech_api-1 --tail 50 2>&1' | Out-File C:/Users/loumk/ssh_out.txt -Encoding utf8; Get-Content C:/Users/loumk/ssh_out.txt"
```

## Reset a test contact (run locally)

```bash
cd /c/Users/loumk/humtech-platform
source .env && DATABASE_URL="$DATABASE_URL" TENANT_ENCRYPTION_KEY="$TENANT_ENCRYPTION_KEY" .venv/Scripts/python.exe scripts/reset_contact.py
```