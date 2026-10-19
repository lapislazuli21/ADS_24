# Stop and remove any leftover client containers from earlier runs of this project
docker ps -aq \
    --filter "label=com.docker.compose.service=client" \
    --filter "label=com.docker.compose.oneoff=True" \
    --filter "label=com.docker.compose.project.working_dir=$PWD" \
    | xargs -r docker rm -f

# Rebuild the client image (picks up changes to client.py) and run it once; --rm removes it afterwards
docker compose -f docker.yaml --profile tools run --rm --build client
