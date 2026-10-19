# Stop and remove only the load balancer container (Redis and the wordcounters keep running)
docker compose -f docker.yaml stop loadbalancer
docker compose -f docker.yaml rm -f loadbalancer

# Rebuild and start the load balancer in the background; --no-deps leaves the wordcounters untouched
docker compose -f docker.yaml up -d --build --no-deps loadbalancer

# Check it's running; the load balancer log should say "Serving on ('0.0.0.0', 18812)"
docker compose -f docker.yaml ps loadbalancer
