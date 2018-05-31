#!/bin/bash

mysql --host=${1}.cdvais0yw4do.us-east-2.rds.amazonaws.com --user=vtuser --password=vtpassword --database=sb -e "${2}"
