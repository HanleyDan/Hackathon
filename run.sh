#!/bin/bash
mvn -q exec:java -Dexec.args="$i" -Dhazelcast.logging.type=none &


