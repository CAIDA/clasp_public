#!/bin/bash
#usage: setupcloud.bash <path to outdir/softwares> <provider> 
LNAME=`logname`
sudo apt-get update;
export DEBIAN_FRONTEND=noninteractive
sudo apt-get install -y build-essential tcpdump  libgbm-dev libpq-dev python-dev  python-pip  python3-dev python3-pip python3-venv python3-wheel gconf-service libasound2 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 ca-certificates fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils wget gnupg1 apt-transport-https dirmngr
cd $1;
#check if scamper exists

if ! [ -x "$(command -v scamper)" ]; then
    wget "https://www.caida.org/tools/measurement/scamper/code/scamper-cvs-20191102b.tar.gz"
    tar xzvf scamper-cvs-20191102b.tar.gz
    patch -s -p0 cloud.patch
    cd scamper-cvs-20191102b;./configure; make; sudo make install;
    sudo ldconfig;
fi
mkdir -p /home/$LNAME/results/bdrmap
mkdir -p /home/$LNAME/results/speedtest

bash /home/$LNAME/outdir/softwares/speedtest/setupspeedtest.bash

if [[ $2 == "amazon" ]]; then sudo apt-get install -y awscli; fi
if [[ $2 == "azure" ]]; then 
    curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash;
    cd /home/$LNAME; wget -O azcopy_v10.tar.gz https://aka.ms/downloadazcopy-v10-linux && tar -xf azcopy_v10.tar.gz --strip-components=1; sudo mv azcopy /usr/local/bin;
    azcopy login --identity
    sudo chown -R $LNAME /home/$LNAME/.azcopy;
fi

#also need to install cron jobs
crontab /home/$LNAME/outdir/softwares/expr.cron
echo "SETUP DONE"
