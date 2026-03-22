# Hostinger Node.js (Git Deploy) Setup

This repo contains a Hostinger-compatible **Express** app:
- `server.js` (Express backend + MySQL)
- `public/index.html` (frontend)

Your old Python/FastAPI (`app.py`) can stay in the repo, but Hostinger will run **Node.js**.

## 1) Prepare the Git repo

Make sure these files exist on branch `main`:
- `package.json`
- `server.js`
- `public/index.html`

## 2) Create the Node.js website in Hostinger

In hPanel:
- Go to **Websites → Node.js**
- Click **Get started**
- Choose **Deploy from GitHub**
- Select the repository and branch (e.g. `main`)

Framework should be detected as **Express**.

## 3) Set environment variables in Hostinger

In the Node.js app settings (Environment variables), add:
- `DB_HOST` = (Hostinger Managed DB host)
- `DB_PORT` = `3306` (unless your panel shows something else)
- `DB_USER` = (db user)
- `DB_PASSWORD` = (db password)
- `DB_NAME` = `energy_market` (or your db name)

Hostinger also sets `PORT` automatically. `server.js` listens on `process.env.PORT`.

## 4) Remote DB access (critical)

In Hostinger **Managed Databases / MySQL** settings, enable remote connections if required:
- If there is an IP whitelist, allow the **Hostinger Node.js runtime IP** (Hostinger may handle this automatically)
- If you can only whitelist “any”, enable it temporarily for testing and then lock it down if Hostinger provides a stable IP range

If you see `/health` returning database errors, this is the first thing to check.

## 5) Test

Open:
- `/health` → should return `{ "status": "healthy", "database": "connected" }`
- `/` → dashboard should load and charts should show

## 6) Update / Redeploy

Push changes to GitHub. In Hostinger Node.js:
- Press **Deploy** (or it redeploys automatically depending on plan)

