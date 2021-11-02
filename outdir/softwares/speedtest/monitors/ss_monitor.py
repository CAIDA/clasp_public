import logging
import subprocess
import platform
from monitor_base import DataSource, SystemObserver, ResultsContainer, _periodic_observer

def get_ss_info():
    os_name = platform.system()
    if 'darwin' in os_name.lower():
        return subprocess.run(['netstat', '-i', '-m'], stdout=subprocess.PIPE).stdout.decode('utf-8')
    else:
        return subprocess.run(['ss', '-i', '-e', '-m', '-p', '-t', '-b'], stdout=subprocess.PIPE).stdout.decode('utf-8')


class NetIfDataSource(DataSource):
    '''
    Monitor `ss` command results
    '''
    def __init__(self):
        DataSource.__init__(self)

        self._results = ResultsContainer()
        self._log = logging.getLogger('mm')

    def __call__(self):
        rd = { 'info': get_ss_info() }
        self._results.add_result(rd)

    @property
    def name(self):
        return 'ss'

    def metadata(self):
        # self._results.drop_first()
        self._log.info("Netstat summary: [yet to be implemented]")
        return self._results.all()

    def show_status(self):
        pass


def create(configdict):
    interval = configdict.pop('interval', 1)
    source = NetIfDataSource()
    return SystemObserver(source, _periodic_observer(interval))
