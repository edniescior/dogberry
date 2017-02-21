# Docker Containers for Cassandra and Kafka

Dogberry has dependencies on Cassandra and Kafka. We use Docker containers to set up these services locally to provide a simple and consistent development environment. They can be accessed from your IDE.

These notes are written for running [Docker for Mac](https://www.docker.com/products/docker#/mac), but will apply to other Docker runtimes. These steps were tested using version:

```
$ docker version
Client:
 Version:      1.13.1
 API version:  1.26
 Go version:   go1.7.5
 Git commit:   092cba3
 Built:        Wed Feb  8 08:47:51 2017
 OS/Arch:      darwin/amd64

Server:
 Version:      1.13.1
 API version:  1.26 (minimum version 1.12)
 Go version:   go1.7.5
 Git commit:   092cba3
 Built:        Wed Feb  8 08:47:51 2017
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

The raw test data set -- `src/test/resources/data/movie_metadata.csv` -- is a subset of the IMDB database and was downloaded from: https://www.kaggle.com/deepmatrix/imdb-5000-movie-dataset.

You will first need to format this data into insert statements by executing the `testdata.MovieDataLoader` object. It will generate the CQL file below. The CQL file will truncate the tables first, so it may be executed multiple times without fear of data corruption.

Once you have generated the CQL file, run this to load the test data:

```
$ docker exec -i -t docker_dogberry-cassandra_1 cqlsh -f '/resources_test/data/insert_eav_test.cql'
```
Warning: Remember to run the create table scripts noted above first.

## Kafka
