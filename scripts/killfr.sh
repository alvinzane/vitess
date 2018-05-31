#!/bin/bash

$VTTOP/examples/local/lvtctl.sh SourceShardDelete sb/80- 0
$VTTOP/examples/local/lvtctl.sh SourceShardDelete sb/-80 0
$VTTOP/examples/local/lvtctl.sh RefreshState cell1-200
$VTTOP/examples/local/lvtctl.sh RefreshState cell1-300
mysql --host=ar0080m.cdvais0yw4do.us-east-2.rds.amazonaws.com --user=vtuser --password=vtpassword -e "delete from _vt.blp_checkpoint"
mysql --host=ar8000m.cdvais0yw4do.us-east-2.rds.amazonaws.com --user=vtuser --password=vtpassword -e "delete from _vt.blp_checkpoint"
