# Stop everything
docker compose -f docker.yaml down

# Build and start Redis and the server in the background
docker compose -f docker.yaml up -d --build

# Check they're running; the server log should say "listening on port 18812"
docker compose -f docker.yaml ps


