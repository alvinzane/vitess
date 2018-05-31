#!/bin/bash

vtworker -topo_implementation zk2 -topo_global_server_address ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813 -topo_global_root /vitess/global -log_dir /home/planetscale/dev/vtdataroot/tmp -alsologtostderr -use_v3_resharding_mode -cell cell1 SplitClone -min_healthy_rdonly_tablets=1 sb/0
