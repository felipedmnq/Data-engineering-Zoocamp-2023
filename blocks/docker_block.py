from prefect.infrastructure.docker import DockerContainer

docker_container_block = DockerContainer.load(
    image="discdiver/felipedmnq:zoocamp",
    image_pull_policy="ALWAYS",
    auto_remove=True
)
    
docker_container_block.save("docker-container", overwrite=True)