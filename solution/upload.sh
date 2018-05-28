#!/usr/bin/env bash
set -e # exit on first error

image_name="git.project-hobbit.eu:4567/florian.schmidt.1994/debs2018solution/system-adapter"

# build image with tag
docker build -t ${image_name} .

# upload image to registry
docker push ${image_name}