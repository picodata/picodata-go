# Description

This is a simple CRUD CLI app implemented using `picodata-go` as the connector  
to a Picodata database.

# Usage

Export the environment variable:

    export PICODATA_CONNECTION_URL='postgres://login:pass@host:port'

Set up a Picodata database. Create the necessary tables:

    psql $PICODATA_CONNECTION_URL < migration.sql

Run the app:

    go run main.go

## Add a string value

    add 'Pico data'

## List items

    list

## Update an item by its id

    update 1 'Learn more Go'

## Delete an item by its id

    remove 1
