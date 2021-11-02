#!/usr/local/bin/bash
KEYLOC="/scratch/webspeedtest/cloud/speedtest_id_rsa"
NODES=""
HOSTSTR=""
HOSTFILE="/scratch/cloudspeedtest/cloudhosts"
STDHOSTFILE="/scratch/cloudspeedtest/standardhosts"
#HNAME=`grep $HOSTFILE /scratch/cloudspeedtest/cloudhosts|awk '{print $2}'`
for arg in "$@"
do
    case $arg in
        -i=*|--include=*)
            NODES=$(echo ${arg#*=}|tr "," "\n")
            shift
            ;;
        -k=*|--key=*)
            KEYLOC="${arg#*=}"
            shift
            ;;
        -f=*|--hostfile=*)
            HOSTFILE="${arg#*=}"
            shift
            ;;
        -h|--help)
            echo "usage: ./batchsetup.bash [-i=host1,host2,...] [-k=privatekey]"
            exit 0
            ;;

    esac
done

if [[ $NODES == "" ]]; then
    #all nodes
    NODES=`awk '{print $1}' $HOSTFILE`
fi

if [[ $NODES != "" ]]; then
    AMAZONHOST=""
    AZUREHOST=""
    GOOGLEHOST=""
    GOOGLESTDHOST=""    
    for node in $NODES
    do
        hname=`grep $node $HOSTFILE |awk '{print $2}'`
#        HOSTSTR="$HOSTSTR -o ${hname}"
        provider=`grep $node $HOSTFILE |awk '{print $3}'`
        case $provider in
            amazon)
                AMAZONHOST="$AMAZONHOST -o ${hname}"
                ;;
            azure)
                AZUREHOST="$AZUREHOST -o ${hname}"
                ;;
            google)
                GOOGLEHOST="$GOOGLEHOST -o ${hname}"
                ;;
            googlestd)
                GOOGLESTDHOST="$GOOGLESTDHOST -o ${hname}"
                ;;
        esac
        #create a text file on each host contained its alias, cannot do in batch
        if [[ $GOOGLESTDHOST == "" ]]; then
            ssh -i $KEYLOC -o StrictHostKeyChecking=no -l ubuntu $hname "echo $node > /home/ubuntu/hostalias"
        else
            ssh -i $KEYLOC -o StrictHostKeyChecking=no -l caida $hname "echo $node > /home/caida/hostalias"            
        fi

    done
    if [[ $AMAZONHOST != "" ]]; then
        orgalorg $AMAZONHOST -k $KEYLOC -u ubuntu  -C bash /home/ubuntu/outdir/softwares/setupcloud.bash /home/ubuntu/outdir/softwares amazon;
    fi
    if [[ $AZUREHOST != "" ]]; then
        orgalorg $AZUREHOST -k $KEYLOC -u ubuntu  -C bash /home/ubuntu/outdir/softwares/setupcloud.bash /home/ubuntu/outdir/softwares azure;
    fi
    if [[ $GOOGLEHOST != "" ]]; then
        orgalorg $GOOGLEHOST -k $KEYLOC -u ubuntu  -C bash /home/ubuntu/outdir/softwares/setupcloud.bash /home/ubuntu/outdir/softwares google;
    fi
    if [[ $GOOGLESTDHOST != "" ]]; then
        orgalorg $GOOGLESTDHOST -k $KEYLOC -u caida  -C bash /home/caida/outdir/softwares/setupcloud.bash /home/caida/outdir/softwares google;
    fi
fi

