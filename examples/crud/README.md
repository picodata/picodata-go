# Description

This is a simple CRUD CLI app implemented using `picodata-go` as the connector  
to a Picodata database.

# Usage

Set up a Picodata database. Run docker images:

    docker-compose up -d

Run the app:

    go run main.go

## Add a string value

    add 'Pico data'

## List items

    list

## Update an item by its id

    update 433242231 'Learn more Go'

## Delete an item by its id

    remove 433242231
