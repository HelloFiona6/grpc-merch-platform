# Assignment 2 - SUSTech Merch Store

Module: Services & API Architectures

## Codebase Structure

Below provides a brief explaination of the files included in this codebase.

- `db-init/`: This folder will be binded to the `docker-entrypoint-initdb.d/` folder inside the `postgres` container. All scripts in this folder will be executed during DB initialization. Check `compose.yaml` to see how it is used.
  - `init.sql`: An SQL script to create all data tables and pre-insert the 3 products into the database.
- `src/`: This main source code folder is where you implement the 3 microservices.
  - `api_service/`: Implement the RESTful API Service as the main backend service. First, update the `openapi.yaml` file to clarify the APIs you will implement. Then, write a [Flask](https://flask.palletsprojects.com/en/stable/)/[FastAPI](https://fastapi.tiangolo.com/) service to implement all the APIs specified in your `openapi.yaml` file.
  - `db_service/`: Implement the DB service with gRPC so that the API Service can interact with it. This folder initially contains a `local_manager.py` file (with its dependency configured in `requirements.txt`) to show how to interact from localhost with the PostgreSQL database inside the `postgres` container. You can consider this file as a tutorial of how to use `psycopg2`. **In your final submission, you should interact with the database from your DB Service container, not localhost!**
  - `logging_service/`: Implement the Logging Service with gRPC so that the API Service can send execution logs to it. This folder initially contains a `local_publisher.py` file (with its dependency configured in `requirements.txt`) to show how to push text messages from localhost to the Kafka topic inside the `kafka` container. You can consider this file as a tutorial of how to use `confluent_kafka`. **In your final submission, you should push log messages from your Logging Service container, not localhost!**
- `.env_example`: An example of what your `.env` file should look like. Refer to the next section to for setup.
- `.gitignore`: Ignores `.env` so your DB user password is not recorded by Git.
- `Makefile`: Contains some utility commands for you. Check the corresponding section for more information.
- `README.md`: Hey, it's me!
- `compose.yaml`: The YAML file where you specify your service containers. We have already configured the PostgreSQL database and the Kafka topic for you and your job is to add your service containers here as well. This file will used by [Docker Compose](https://docs.docker.com/compose/intro/compose-application-model/#the-compose-file).

## Setup

### Specify `.env`

Duplicate `.env_example` and rename it to `.env`. Configure the PostgreSQL credentials inside this file. It will be used by `compose.yaml`.

```yaml
POSTGRES_USER: ${POSTGRES_USER}
POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
POSTGRES_DB: ${POSTGRES_DB}
```

## Utility Commands in `Makefile`

We offer some utility bash commands in `Makefile`. Feel free to use them during your development & experiment.

- `setup-psql` uses `apt install` to configure the [`psql`](https://www.postgresql.org/docs/current/app-psql.html) CLI and then starts a PostgreSQL server for its connection. Use this command if you want to connect to the database directly from localhost.
- `how-to-psql` shows how to connect to the database from within the `postgres` container.
- `how-to-relife-db` shows how to clean up the database (and its binded volume) when stopping the with `docker compose`.
- `how-to-stream-log` shows how to connect to the Kafka topic and fetch logs from it in a streaming fashion. Essentially, we pop out a [Kafka console consumer](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html#kafka-console-consumer-sh) inside the `kafka` container to connect to the topic.
