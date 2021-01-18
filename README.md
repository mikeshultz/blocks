# blocks

For the insane person who wants the blockchain in a relational database(PostgreSQL).

## Install

    pip install setup.py

## Configuration

Either entries in an INI file like so, or environmental variables.  The latter takes precidence in
all cases.

### Environmental Variables

 - LOG_LEVEL
 - JSONRPC_NODE
 - PGUSER
 - PGPASSWORD
 - PGHOST
 - PGPORT
 - PGDATABASE

## Deploy

### ECS

1) Build the docker image

    docker build -t blocks:v0.0.1b1 --build-arg DB_USER="blocks" --build-arg DB_PASS="pASS" --build-arg DB_HOST="data.box.gointo.tools" --build-arg JSONRPC_NODE="https://node0.eth.gointo.software/" .

2) Login to AWS ECR

    $(aws --profile gointo ecr get-login --no-include-email --region us-west-2)

3) Tag the image

    docker tag blocks 525686199231.dkr.ecr.us-west-2.amazonaws.com/blocks:v0.0.1b1

4) Push the image

    docker push 525686199231.dkr.ecr.us-west-2.amazonaws.com/blocks:v0.0.1b1

5) Update the Terraform task definition for this app and apply.
