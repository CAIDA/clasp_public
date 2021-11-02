#!/bin/sh
mkdir /home/ubuntu/outdir
cp -r /home/ubuntu/scratch/cloudspeedtest/outdir/* /home/ubuntu/outdir/
echo "amazon"> /home/ubuntu/vmtype
mv /home/ubuntu/outdir/datafiles/amazon.sibling.txt /home/ubuntu/outdir/datafiles/amazon.sibling.active
rm -rf /home/ubuntu/scratch 
