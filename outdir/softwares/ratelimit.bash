#!/usr/bin/bash
LNAME=`logname`
CAIDA="192.172.226.0/24"
STORAGE=""
if [[ $LNAME == "" ]]; then
    LNAME="ubuntu"
fi
HNAME=`cat /home/$LNAME/vmtype`
if [[ $HNAME == "amazon" ]]; then
    STORAGE="52.192.0.0/11"
fi

if [[ $HNAME == "azure" ]]; then
    STORAGE="52.224.0.0/11"
fi

if [[ $HNAME == "google" ]]; then
    STORAGE="172.217.0.0/16"
fi

if [[ $HNAME == "googlestd" ]]; then
    STORAGE="172.217.0.0/16"
fi

PRIETH=`ls -1 /sys/class/net/ |head -n1`
INETH="ifb0"

SPBWLIMIT="100mbit"
SPINBWLIMIT="1000mbit"
echo "Setting Rate limit to $PRIETH. $HNAME $STORAGE"

sudo modprobe ifb numifbs=1

echo "Removing old rules"
sudo tc qdisc del dev $PRIETH root
sudo tc qdisc del dev $PRIETH ingress
sudo tc qdisc del dev $INETH root
sudo tc qdisc del dev $INETH ingress


echo "Adding ingress rules"
#Ingress
sudo tc qdisc add dev $PRIETH handle ffff: ingress

sudo ip link set dev $INETH up
sudo tc filter add dev $PRIETH parent ffff: protocol all u32 match u32 0 0 action mirred egress redirect dev $INETH

echo "Adding ingress qdisc"
#we expect not much other ingress traffic, except speedtest traffic
sudo tc qdisc add dev $INETH root handle 1: htb default 13
echo "Adding ingress classes"
sudo tc class add dev $INETH parent 1: classid 1:1 htb rate 2000mbit ceil 2000mbit
echo "Adding ingress ICMP class"
sudo tc class add dev $INETH parent 1:1 classid 1:11 htb rate 2000mbit ceil 2000mbit
sudo tc filter add dev $INETH protocol ip parent 1:0 prio 0 u32 match ip protocol 1 0xff flowid 1:11

echo "Adding ingress caida class"
sudo tc class add dev $INETH parent 1:1 classid 1:12 htb rate 2000mbit ceil 2000mbit
sudo tc filter add dev $INETH protocol ip parent 1:0 prio 5 u32 match ip src $CAIDA flowid 1:12
echo "Adding ingress default class"
sudo tc class add dev $INETH parent 1:1 classid 1:13 htb rate $SPINBWLIMIT ceil $SPINBWLIMIT


echo "Adding egress rules"
#Engress
sudo tc qdisc add dev $PRIETH root handle 1: htb default 13
#all traffic
sudo tc class add dev $PRIETH parent 1: classid 1:1 htb rate 4000mbit ceil 4000mbit

echo "Adding egress storage class"
#cloud storage traffic low priority
sudo tc class add dev $PRIETH parent 1:1 classid 1:10 htb rate 2000mbit ceil 2000mbit
sudo tc filter add dev $PRIETH protocol ip parent 1:0 prio 5 u32 match ip dst $STORAGE flowid 1:10

echo "Adding egress caida class"
#caida traffic
sudo tc class add dev $PRIETH parent 1:1 classid 1:11 htb rate 2000mbit ceil 2000mbit
sudo tc filter add dev $PRIETH protocol ip parent 1:0 prio 5 u32 match ip dst $CAIDA flowid 1:11

echo "Adding egress UDP class"
sudo tc class add dev $PRIETH parent 1:1 classid 1:12 htb rate 2000mbit ceil 2000mbit
sudo tc filter add dev $PRIETH protocol ip parent 1:0 prio 0 u32 match ip protocol 17 0xff flowid 1:12

echo "Adding egress default class"
#other (measurement traffic)
sudo tc class add dev $PRIETH parent 1:1 classid 1:13 htb rate $SPBWLIMIT ceil $SPBWLIMIT


#**********************************************************************
#........-................-...........................................
#.......---..............---.........................................
#......--O--............--O--........................................
#.......---..............---.........................................
#........-....../\........-.........................................
#...............--..................................................
#....................................................................
#........|----------------|..........................................
#.............................................Hello world............
#********************************************************************

