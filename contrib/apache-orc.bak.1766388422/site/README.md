# Apache ORC docs site

This directory contains the code for the Apache ORC web site,
[orc.apache.org](https://orc.apache.org/). The easiest way to build
the site is to use docker to use a standard environment.

## Setup

1. `cd site`
2. `git clone git@github.com:apache/orc.git -b asf-site target`

## Run the docker container with the preview of the site.

1. `docker run -d --name orc-container -p 4000:4000 -v $PWD:/home/orc/site apache/orc-dev:site`

## Browsing

Look at the site by navigating to
[http://0.0.0.0:4000/](http://0.0.0.0:4000/) .

## Pushing to site

You'll copy the files from the container to the site/target directory and
commit those to the asf-site branch.

1. `docker cp orc-container:/home/orc/site/target .`
2. `cd target`
3. Commit the files and push to Apache.

## Shutting down the docker container

1. `docker rm -f orc-container`
