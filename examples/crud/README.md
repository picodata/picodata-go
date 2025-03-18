# Description

This is a simple CRUD cli-app implemented using picodata-go as the connector
to a Piscodata database.

# Usage
Set an env varibale

    export PICODATA_CONNECTION_URL='postgres://login:pass@host:port'

Setup a Picodata database. Create the necessary data schema:

    psql $PICODATA_CONNECTION_URL < migration.sql

Build app:

    go build

Run app:

    ./crud

## Add a string

    add 'Pico data'

## List items

    list

## Update an item

    update 1 'Learn more go'

## Delete an item

    remove 1
