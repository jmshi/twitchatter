#!/bin/bash
/usr/local/cassandra/bin/nodetool status
/usr/local/cassandra/bin/nodetool ring |more
