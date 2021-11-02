#class for retreiving data from Amazon, Google cloud and Azure and running cloud analysis
import boto3
import time
import threading
import subprocess
from subprocess import check_output
import os
import concurrent.futures

class retreive:
	def __init__(self, interval, vmtype, hostalias,local_dir):
		self.vmtype = vmtype
		self.hostalias = hostalias
		self.interval = interval
		self.local_dir = local_dir
		self.read_hosts()
		if self.vmtype == "amazon":
			self.client = boto3.client('s3')
			self.s3 = boto3.resource('s3')
			self.bucket = "cloudspeedtest"
		elif self.vmtype == "azure":
			subprocess.run(["azcopy","login", "--identify"])
		elif self.vmtype == "google":
			pass
		self.retreive_timer = threading.Timer(self.interval, self.retreive)
		self.retreive_timer.start()
	
	def read_hosts(self):
		self.hosts = []
		with open('cloudhosts') as f:
			hosts = f.readlines()
		for h in hosts:
			if h.split(' ')[2].split('\n')[0] == self.vmtype:
				self.hosts.append(h.split(' ')[0])	

	def retreive(self):
		file_count = 0
		print(self.vmtype)
		if self.vmtype == "amazon":
			# to retreive data from aws s3 and then archive retreived data to the old folderos.path.join(['tmp', item_uuid, key]))
			for h in self.hosts:
				if file_count > 3000:
					break
				ret_files = self.client.list_objects(Bucket=self.bucket, Prefix = h+'/')
				new_file_list = self.aws_files(ret_files)
				file_count += len(new_file_list)

				for f in new_file_list: #
					download_path = os.path.join(self.local_dir, f)
					download_dir = download_path.rsplit('/',1)[0]
					if not os.path.exists(download_dir):
						os.makedirs(download_dir)
					print(download_path,download_dir)
					self.s3.Bucket(self.bucket).download_file(f, download_path)
				
				self.archive_cloud(new_file_list,h)
				self.analyze_data(new_file_list)

		elif self.vmtype == "azure":
			for h in self.host:
				if file_count > 2000:
					break
				ret_files = check_output(["azcopy", "ls", "https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + h + "/speedtest"]).decode()
				new_file_list = self.azure_files(ret_files)
				file_count += len(new_file_list)
				for f in new_file_list:
					download_dir = os.path.join(self.local_dir, h, "speedtest")
					if not os.path.exists(download_dir):
						os.makedirs(download_dir)
					subprocess.run(["azcopy","copy","https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + h + "/speedtest" + f, download_dir])

				self.archive_cloud(new_file_list,h)
				self.analyze_data(new_file_list)
		
		elif self.vmtype == "google":
			for h in self.hosts:
				if file_count > 2000:
					break
				ret_files = check_output(["gsutil", "ls", "-r", "gs://cloudspeedtest/" + h + "/results/speedtest/"]).decode()
				new_file_list = self.gcloud_files(ret_files)
				file_count += len(new_file_list)
				for f in new_file_list:
					download_dir = os.path.join(self.local_dir, h , "speedtest")
					if not os.path.exists(download_dir):
						os.makedirs(download_dir)
					subprocee.run(["gsutil", "cp", "-r", f, download_dir])
				
				self.archive_cloud(new_file_list,h)
				self.analyze_data(download_dir)

	def aws_files(self,ret_files):
		new_file_list = []		
		for f in ret_files['Contents']:
			new_file_list.append(f['Key'])
		return new_file_list

	def azure_files(self,ret_files):
		files = ret_files.split('\n')[1:]
		new_files = []
		for f in files:
			new_files.append(f.split(' ')[1])
		return new_files

	def gcloud_files(self,ret_files):
		files = ret_files.split('\n')[1:]
		return files


	def archive_cloud(self,flist,host):
		if self.vmtype == "amazon":
			for f in flist:
				self.s3.Object(self.bucket,'archive/'+f).copy_from(CopySource=self.bucket+'/'+f)
				self.s3.Object(self.bucket,f).delete()
		elif self.vmtype == "azure":
			for f in flist:
				subprocess.run(["azcopy","copy","https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + host + "/speedtest" + f, 
					"https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/archive/" + host + "/speedtest" + f])
				subprocess.run(["azcopy","remove", "https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + host + "/speedtest" + f])
		elif self.vmtype == "google":
			for f in flist:
				pre_file = "gs://cloudspeedtest/"
				sub_file = f.split(pre_file)
				subprocess.run(["geisutil", "mv", f, pre_file + "archive/" + sub_file[1]])

	# analyze, remove, and upload data
	def analyze_data(self, download_dir):
		rm_file_list = {}
		tar_list = check_output(["find", download_dir, "-type", "f", "-name", "*.tar.bz2"]).decode().split('\n')
		for f in tar_list:
			file_list = check_output(["tar", "-xvf", f]).decode().split('\n')
			subprocess.run(["rm",f])

			# compose file path to be removed  
			rm_dir = f.rsplit('/',1)[0]
			for file in file_list:
				rm_file_list[rm_dir + '/' + file] = f

		subprocess.run(["go","run","cloudanalysis.go"])
		self.rm_local(rm_file_list)
		self.upload_data(rm_file_list)


	def rm_local(self, rm_file_list):
		for f in rm_file_list:
			subprocess.run(["rm", f]) #remove json,pcap,web.csv and png.

	def upload_data(self, rm_file_list):
		# zip pcap, json, rttjson, lostjson and plots and upload to the storage cloud
		prefix_set = set()
		for f in rm_file_list: 
			# e.g. f= "/Users/yangrui/data/aws-oh-1/speedtest/outdir/softwares/speedtest/comcast/comcast_1588360880.6140668.json"
			new_prefix = f.split('.')[0]
			prefix_set.add(new_prefix)
		for p in prefix_set:
			gob_file = p + '.gob'
			lost_file = p + '.lost.json'
			rtt_file = p + '.rtt.json'
			
			plot_dir = p.rsplit('/',1)[0] + '/plots' 
			plot_files = check_output(["find", plot_dir, "-type", "f", "-name", p+'*']).decode().split('\n')

			zip_name = rm_file_list[]

if __name__ == "__main__":
	vmtype = check_output(["cat", "/home/ubuntu/vmtype"]).decode().split('\n')[0]
	hostalias = check_output(["cat", "/home/ubuntu/hostalias"]).decode().split('\n')[0]
	retreive_model = retreive(3600, vmtype, hostalias, "/home/ubuntu/data")
	# retreive_model.retreive()

