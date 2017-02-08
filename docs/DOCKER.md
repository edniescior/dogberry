# Docker Containers for Cassandra and Kafka

Dogberry has dependencies on Cassandra and Kafka. We use Docker containers to set up these services locally to provide a simple and consistent development environment. They can be accessed from your IDE.

These notes are written for running [Docker for Mac](https://www.docker.com/products/docker#/mac), but will apply to other Docker runtimes. These steps were tested using version:

```
$ docker version
Client:
 Version:      1.13.0
 API version:  1.25
 Go version:   go1.7.3
 Git commit:   49bf474
 Built:        Wed Jan 18 16:20:26 2017
 OS/Arch:      darwin/amd64

Server:
 Version:      1.13.0
 API version:  1.25 (minimum version 1.12)
 Go version:   go1.7.3
 Git commit:   49bf474
 Built:        Wed Jan 18 16:20:26 2017
 OS/Arch:      linux/amd64
 Experimental: true
 ```

We use Docker Compose to start and stop the containers. The `docker-compose.yml` file is under `src/test/resources/docker`.

```
$ cd src/test/resources/docker
```

Before starting the containers edit the environment variable file `.env` and set the variables to match your environment. For example:

```
DOG_SRC_DIR=/users/your_user/your_repos/dogberry/src
```

Use Docker Compose to start the containers.

```
$ docker-compose up -d
```

## Cassandra

 The Cassandra create scripts have been mounted to the `/resources_main` and `/resources_test` volumes on the container. Execute these scripts to create the keyspaces and tables:

```
$ docker exec -i -t docker_dogberry-cassandra_1 cqlsh -f '/resources_main/DDL/create_tables_dogberry.cql'
$ docker exec -i -t docker_dogberry-cassandra_1 cqlsh -f '/resources_test/DDL/create_tables_test.cql'
```

To stop the containers:

```
$ docker-compose stop
```

To clean up exited containers:

```
$ docker rm -v $(docker ps -aq -f status=exited)
```

Do this if you want to start off fresh next time you start up the containers. You will need to recreate the keyspaces and tables in Cassandra (see above).

To log in to `cqlsh` on the Cassandra container:

```
$ docker exec -i -t docker_dogberry-cassandra_1 cqlsh
```

To log in to a bash shell on the Cassandra container:

```
$ docker exec -i -t docker_dogberry-cassandra_1 /bin/bash
```

#### Seeding Cassandra with Test Data

The raw test data set is a subset of the IMDB database and is available here: https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset.

To load the test data:

```
$ docker exec -i -t docker_dogberry-cassandra_1 cqlsh -f '/resources_test/data/insert_eav_test.cql'
```
(Note: The `testdata.MovieDataLoader` object was used to create the above files.)

## Kafka
