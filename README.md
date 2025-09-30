# Corderos App

Local development setup for the FastAPI application that uses PostgreSQL.

## Local Postgres (Docker)

1. Start the database (first run downloads the image):
   ```bash
   docker compose -f docker-compose.postgres.yml up -d
   ```
2. Wait for the container health check to report `healthy`. You can monitor with:
   ```bash
   docker compose -f docker-compose.postgres.yml ps
   ```
3. Export the connection string so the app can reach the database and configure the session secret:
   ```bash
   export DATABASE_URL="postgresql://corderos_app:corderos_pass@localhost:55432/corderos"
   export SESSION_SECRET="change-me"  # use a long, random value in production
   export SESSION_COOKIE_SECURE="false"  # allow HTTP during local development
   ```
4. Run the FastAPI server:
   ```bash
   uvicorn app.main:app --reload --port 8000
   ```
5. When finished, stop the database (use `-v` if you want to drop the data volume):
   ```bash
   docker compose -f docker-compose.postgres.yml down
   ```

## PSQL Access

With the container running you can inspect the data directly:
```bash
psql postgresql://corderos_app:corderos_pass@localhost:55432/corderos
```

## Troubleshooting

- If the schema changes, remove the volume so the init script re-runs:
  ```bash
  docker compose -f docker-compose.postgres.yml down -v
  docker compose -f docker-compose.postgres.yml up -d
  ```
- Make sure port `55432` is free before starting the container.
