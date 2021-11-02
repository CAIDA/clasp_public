#for getting webcsv results (download/upload/latency) from the cloud.
#Usage: python3 retreive_webcsv.py <start_time_of_exp> <end_time_of_exp>
import boto3
import time
import threading
import subprocess
from subprocess import check_output
import os
import concurrent.futures
import sys

class webdata:
	def __init__(self, vmtype, hostalias, local_dir,s_ctime,e_ctime):
		self.vmtype = vmtype
		self.hostalias = hostalias
		self.local_dir = local_dir
		self.start_time = s_ctime
		self.end_time = e_ctime
		if self.vmtype == "amazon":
			self.client = boto3.client('s3')
			self.s3 = boto3.resource('s3')
			self.bucket = "cloudspeedtest"
			self.read_hosts()

		elif self.vmtype == "azure":
			self.azure_log()
			self.log_timer = threading.Timer(3600, self.azure_log)
			self.log_timer.start()
			self.get_azure_host_files()
			self.get_host_files()
		
		elif self.vmtype == "google":
			pass

		self.concurrent_retreive()
	
	def azure_log(self):
		subprocess.run(["azcopy","login", "--identity"])

	def read_hosts(self):
		self.hosts = set()
		with open('cloudhosts') as f:
			hosts = f.readlines()
		for h in hosts:
			if h.split(' ')[2].split('\n')[0] == self.vmtype:
				self.hosts.add(h.split(' ')[0])	
	
	def get_azure_host_files(self):
		self.host_files = {}
		for h in self.hosts:
			ret_files = check_output(["azcopy", "ls", "https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + h + "/speedtest"]).decode()
			self.host_files[h] = self.azure_files(ret_files)
			print('successfully retreive file list from ', h)
							

	def azure_files(self,ret_files):
		files = ret_files.split('\n')[1:]
		new_files = []
		for f in files:
			try:
				new_files.append(f.split(' ')[1].split(';')[0])
			except Exception as exp:
				if f=='':
					pass
				else:
					print('Error when retreiving file list', exp)
		return new_files


	def aws_files(self, aws_host):
		new_file_list = []		
		for f in self.get_all_s3_ojects(Bucket=self.bucket, Prefix = aws_host+'/'):
			new_file_list.append(f['Key'])
		return new_file_list

	def gcloud_files(self,ret_files):
		files = ret_files.split('\n')[1:]
		return files

	# list_objects only return 1000 objects from S3, this func is to retreive the whole file list.
	def get_all_s3_ojects(self, **base_kwargs):
		continuation_token = None
		while True:
			list_kwargs = dict(MaxKeys=1000,**base_kwargs)
			if continuation_token:
				list_kwargs['ContinuationToken'] = continuation_token
			response = self.client.list_objects_v2(**list_kwargs)
			yield from response.get('Contents', [])
			if not response.get('IsTruncated'):
				break
			continuation_token = response.get('NextContinuationToken')


	def concurrent_retreive(self):
		with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.hosts)) as executor:
			future_to_host = {executor.submit(self.retreive_webdata, host): host for host in self.hosts}
			for future in concurrent.futures.as_completed(future_to_host):
				try:
					host = future_to_host[future]
					print(future.result())
				except Exception as exp:
					print(exp)

	def retreive_webdata(self,host):
		# to retreive，unzip data from aws s3 and then extract speedtest results from web.csv files
		if self.vmtype == "amazon":
			h = host
			print('aws server name: ', h)
			new_file_list = self.aws_files(h)
			
			for f in new_file_list: #
				file_time = int(f.split('.')[1])
				if 'bdr' in f or file_time < self.start_time or file_time > self.end_time:
					continue
				download_path = os.path.join(self.local_dir, f)
				download_dir = download_path.rsplit('/',1)[0]
				if not os.path.exists(download_dir):
					os.makedirs(download_dir)
				print(download_path,download_dir)
				self.s3.Bucket(self.bucket).download_file(f, download_path)
				
				try:
					#unzip
					ip_slices = download_path.split('.')
					test_ip_addr = ip_slices[2] + '.' + ip_slices[3] + '.' + ip_slices[4] + '.' + ip_slices[5]
					test_platform = ip_slices[6]
					target_files = check_output(["tar","-xvf",download_path]).decode().split('\n')
					del target_files[-1]
					# print('target' ,target_files)
					test_time = target_files[0].split('_')[-1].split('.')[0]
					for t in target_files:
						if 'web.csv' in t:
							web_file = t

							with open(web_file,'r') as rfile:
								content = rfile.readlines()
							web_result = [test_platform + '; ', test_time + '; ', test_ip_addr + '; ', content[0]+'\n']
							with open(self.local_dir + '/' + h + '.txt' ,'a') as wfile:
								wfile.writelines(web_result)
							break
					#delete
					subprocess.run(["rm", download_path])
					for t in target_files:
						subprocess.run(["rm", t])
				
				except Exception as exp:
						print(exp)

			return_note = 'finish retreiving web.csv data for ' + h
			return return_note

		elif self.vmtype == "azure":
			h = host
			print('azure server name: ', h)
			new_file_list = self.host_files[h]
			# ret_files = check_output(["azcopy", "ls", "https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + h + "/speedtest"]).decode()
			# print(ret_files)
			# new_file_list = self.azure_files(ret_files)
			for f in new_file_list:
				
				file_time = int(f.split('.')[1])
				if 'bdr' in f or file_time < self.start_time or file_time > self.end_time:
					continue

				download_dir = os.path.join(self.local_dir, h, "speedtest")
				if not os.path.exists(download_dir):
					os.makedirs(download_dir)
				cloud_path = "https://cloudspeedtestblob.blob.core.windows.net/cloudspeedtestcontainer/" + h + "/speedtest/" + f
				download_path = download_dir + '/' + f
				subprocess.run(["azcopy","copy",cloud_path, download_dir])
				
				try:
					#unzip
					ip_slices = download_path.split('.')
					test_ip_addr = ip_slices[2] + '.' + ip_slices[3] + '.' + ip_slices[4] + '.' + ip_slices[5]
					test_platform = ip_slices[6]
					target_files = check_output(["tar","-xvf",download_path]).decode().split('\n')
					del target_files[-1]
					# print('target' ,target_files)
					test_time = target_files[0].split('_')[-1].split('.')[0]
					for t in target_files:
						if 'web.csv' in t:
							web_file = t
							with open(web_file,'r') as rfile:
								content = rfile.readlines()
							web_result = [test_platform + '; ', test_time + '; ', test_ip_addr + '; ', content[0]+'\n']
							with open(self.local_dir + '/' + h + '.txt' ,'a') as wfile:
								wfile.writelines(web_result)
							break
					#delete
					subprocess.run(["rm", download_path])
					for t in target_files:
						subprocess.run(["rm", t])
				
				except Exception as exp:
						print(exp)
			return_note = 'finish retreiving web.csv data for ' + h
			return return_note

		elif self.vmtype == "google": 
			h = host
			print('google cloud server name: ', h)
			ret_files = check_output(["gsutil", "ls", "-r", "gs://cloudspeedtest/" + h + "/results/speedtest/"]).decode()
			new_file_list = self.gcloud_files(ret_files)
			for f in new_file_list:
				# only download files with exps running between start time and end time
				file_time = int(f.split('.')[1])
				if 'bdr' in f or file_time < self.start_time or file_time > self.end_time:
					continue
				
				download_dir = os.path.join(self.local_dir, h , "speedtest")
				download_path = os.path.join(download_dir, f.split('/')[-1])
				if not os.path.exists(download_dir):
					os.makedirs(download_dir)
				subprocess.run(["gsutil", "cp", "-r", f, download_dir])
				
				try:
					#unzip
					ip_slices = download_path.split('.')
					test_ip_addr = ip_slices[2] + '.' + ip_slices[3] + '.' + ip_slices[4] + '.' + ip_slices[5]
					test_platform = ip_slices[6]
					target_files = check_output(["tar","-xvf",download_path]).decode().split('\n')
					del target_files[-1]
					# print('target' ,target_files)
					test_time = target_files[0].split('_')[-1].split('.')[0]
					for t in target_files:
						if 'web.csv' in t:
							web_file = t

							with open(web_file,'r') as rfile:
								content = rfile.readlines()
							web_result = [test_platform + '; ', test_time + '; ', test_ip_addr + '; ', content[0]+'\n']
							with open(self.local_dir + '/' + h + '.txt' ,'a') as wfile:
								wfile.writelines(web_result)
							break
					#delete
					subprocess.run(["rm", download_path])
					for t in target_files:
						subprocess.run(["rm", t])
				
				except Exception as exp:
						print(exp)

			return_note = 'finish retreiving web.csv data for ' + h
			return return_note


if __name__ == "__main__":
	vmtype = check_output(["cat", "/home/ubuntu/vmtype"]).decode().split('\n')[0]
	hostalias = check_output(["cat", "/home/ubuntu/hostalias"]).decode().split('\n')[0]
	# webdata_model = webdata(vmtype, hostalias, "/home/ubuntu/webdata", 1588870000, 1589500000)
	# webdata_model = webdata(vmtype, hostalias, "/home/ubuntu/webdata", 1589054521, 1589500000)
	webdata_model = webdata(vmtype, hostalias, "/home/ubuntu/webdata", sys.argv[1], sys.argv[2])



