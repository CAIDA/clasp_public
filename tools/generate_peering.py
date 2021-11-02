## Version 4 created on 20200224
## Parses the IXP file extracted from /data/external/topology-asdata/ixps/ixs_*.jsonl
## Creates the .peering files (v4 and v6) used as inputs of sc_bdrmap
##
## Notes: To execute it, run the command:
## "python 1_generate_peering_file_v4.py /data/external/topology-asdata/ixps/ixs_201907.jsonl"
## In case you run "python 1_generate_peering_file_v3.py", it finds out the last ix_file in the
## directory /data/external/topology-asdata/ixps

from pprint import pprint
from datetime import datetime
import json, sys, os
reload(sys)
sys.setdefaultencoding('utf8')

def find_latest_ixs_file():
    """ returns the the latest available file of the directory
        /data/external/topology-asdata/ixps that we could use """
    current_time = datetime.now()
    
    default_name = "/data/external/topology-asdata/ixps/ixs_201910.jsonl"
    print ('Year - current_time  = ', current_time.year, current_time.month)
    for year in range(int(current_time.year), 2000, -1):
        for month in range(12, 0, -1):
            print('YM = ', year, month)
            potential_name = '/data/external/topology-asdata/ixps/' + 'ixs_' + str(year) + str(month) + '.jsonl'
            if os.path.exists(potential_name):
                print 'potential_name = ', potential_name
                return potential_name, str(year), str(month)
    return default_name, 2019, 10




if __name__ == '__main__':
    if len(sys.argv) > 1:
        file_to_parse = sys.argv[1]
        # We assume that the file is under the format ixs_yyyymm.jsonl
        tab = file_to_parse.split('/')
        cyear = tab[-1][4:8]
        cmonth = tab[-1][8:10]
        print('cyear =', cyear, 'cmonth = ', cmonth)
        #sys.exit('OUT')
    else:
        file_to_parse, cyear, cmonth = find_latest_ixs_file()

    List_prefixes = []
    with open (cyear + cmonth +'.v4.peering', 'w') as fv4:
        with open (cyear + cmonth +'.v6.peering', 'w') as fv6:
            #with open('/data/external/topology-asdata/ixps/ixs_201907.jsonl', 'r') as fg:
            with open (file_to_parse , 'r') as fg:
                for line in fg:
                    if '#' not in line:
                        print line
                        data = json.loads(line)

                        print
                        print(data['prefixes'])

                        if 'ipv4' in data['prefixes']:
                            i = 0
                            while i < len(data['prefixes']['ipv4']):
                                pref = data['prefixes']['ipv4'][i]
                                name = data['name'].upper()
                                if pref not in List_prefixes:
                                    fv4.write('%s %s \n' %( pref, str(name.encode("utf-8")) ))
                                    List_prefixes.append(pref)
                                i+=1

                        if 'ipv6' in data['prefixes']:
                            i = 0
                            while i < len(data['prefixes']['ipv6']):
                                pref = data['prefixes']['ipv6'][i]
                                name = data['name'].upper()
                                if pref not in List_prefixes:
                                    fv6.write('%s %s \n' %( pref, str(name.encode("utf-8")) ))
                                    List_prefixes.append(pref)
                                i+=1