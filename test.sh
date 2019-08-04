#!/bin/bash

go build test.go

goreman -i 2s start
