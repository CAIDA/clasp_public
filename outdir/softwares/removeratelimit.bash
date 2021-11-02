#!/usr/bin/bash
PRIETH=`ls -1 /sys/class/net/ |head -n1`
INETH="ifb0"
echo "Removing old rules"
sudo tc qdisc del dev $PRIETH root
sudo tc qdisc del dev $PRIETH ingress
sudo tc qdisc del dev $INETH root
sudo tc qdisc del dev $INETH ingress

