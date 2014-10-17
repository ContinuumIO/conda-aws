from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.exceptions import NetworkError

import boto.vpc
import sys
import socket
import os, os.path
from time import sleep

def create_vpc(rgn,vpc_snet,subnet_arg,avzone_arg):
	def fix_subnet_ip(subnet_arg):
		# go from 10.10.0.0/24 to 10.10.0.181 (for example)
		# we do not check the validity of the input arg
		s = subnet_arg.split(".")
		if s and len(s)>=3:
			return s[0]+'.'+s[1]+'.'+s[2]+'.181'
		else:
			return None

	# region is outside parameter like "us-east-1"
	connvpc = boto.vpc.connect_to_region(rgn)

	# vpc subnet is parameter like "10.10.0.0/16"
	myvpc = connvpc.create_vpc(vpc_snet)
	# subnet within vpc is another arg like 10.10.0.0/24
	# availability zone is within region, like us-east-1c
	snet = connvpc.create_subnet(vpc_id=myvpc.id,cidr_block=subnet_arg,availability_zone=avzone_arg)
	# create an internet gateway for this VPC
	mygw = connvpc.create_internet_gateway()
	# attache the gateway to the VPC
	connvpc.attach_internet_gateway(mygw.id,myvpc.id)
	# create a default routing table for the VPC
	# for that we need to find the route attached to our vpc
	rt_found = False
	routes = connvpc.get_all_route_tables()
	for r in routes:
		if r.vpc_id == myvpc.id:
			rt_found = True
			break
	if not rt_found:
		print 'Problem setting up new VPC route.'
		sys.exit(-1)
	myrt = connvpc.create_route(r.id,"0.0.0.0/0",mygw.id)
	# create a basic security group for the VPC
	sgvpn = connvpc.create_security_group("spark-vpn",description="spark-vpn",vpc_id=myvpc.id)
	# authorize ssh and openvpn traffic for the group
	sgvpn.authorize("tcp",22,22,"0.0.0.0/0")
	sgvpn.authorize("udp",1194,1194,"0.0.0.0/0")
	# the openvpn instance we will be running will have two addresses: an internal IP attached to the VPC (like 10.8.0.100)
	# and an external IP accessible from anywhere so that we can connect to the VPC via an openvpn client
	# we force this allocated_address to be the subnet passed in .181 (just chose it randomly ;)
	private_ip = fix_subnet_ip(subnet_arg)
	# here we create the openvpn instance with the appropriate security group and private ip address
	# it is based off the basic Ubuntu 14.04 AMI offered by Amazon
	resvs = connvpc.run_instances("ami-e84d8480",min_count=1,max_count=1,key_name="beast",instance_type="t1.micro",subnet_id=snet.id,private_ip_address=private_ip,security_group_ids=[sgvpn.id])
	i0 = resvs.instances[0]
	print 'Starting instance.'
	while i0.update() != 'running':
		sleep(3)
	ready = False
	while not ready:
		i0.update()
		sts = connvpc.get_all_instance_status([i0.id])[0]
		if sts.instance_status.status != 'ok' or sts.system_status.status != 'ok':
			sleep(3)
		else:
			ready = True

	print 'Instance started.Sleeping for a few seconds to allow ssh to start up...'
	sleep(5)
	# the instance needs to have sourceDestCheck turned off
	connvpc.modify_instance_attribute(i0.id,attribute="sourceDestCheck",value="false")
	# and here we associate an external IP address to it
	eip = connvpc.allocate_address("vpc")
	connvpc.associate_address(i0.id,eip.public_ip,eip.allocation_id)

	return (private_ip,eip.public_ip)

# this function hard-codes the openvpn network to be
# the private 10.8.0.0/24 space, makes it easier
def create_server_conf_file(private_ip):
	def fix_ip():
		s = private_ip.split('.')
		new_s = s[0]+'.'+s[1]+'.'+s[2]+'.0'
		print 'Private subnet for openvpn route set to %s' % new_s
		return new_s

	server_contents = """
		local 0.0.0.0
		proto udp
		dev tun
		ca /etc/openvpn/server/ca.crt
		cert /etc/openvpn/server/server.crt
		key /etc/openvpn/server/server.key
		dh /etc/openvpn/server/dh2048.pem
		server 10.8.0.0 255.255.255.0
		ifconfig-pool-persist /etc/openvpn/server/ipp.txt
		push "route %s 255.255.255.0"
		client-to-client
		keepalive 10 120
		comp-lzo
		persist-key
		persist-tun
		status /etc/openvpn/server/openvpn-status.log
		verb 3""" % fix_ip()

	try:
		f = open("server.conf","w")
		f.write(server_contents)
		f.close()
	except IOError, ioe:
		print ioe
		sys.exit(-1)

def main(region,vpc_snet,subnet_arg,avzone_arg,ssh_key,num_slaves,hdfs_config):
	if vpc_snet.startswith("10.8"):
		print "Subnet 10.8.x.x is reserved for the VPN client network. Use any other private space subnet please."
		sys.exit()

	if hdfs_config and num_slaves < 2:
		print "More than 3 machines needed in cluster configuration for HDFS"
		sys.exit()

	private_ip,public_ip = create_vpc(region,vpc_snet,subnet_arg,avzone_arg)
	create_server_conf_file(private_ip)
	env.hosts=[public_ip]
	can_ssh = False
	while not can_ssh:
		try:
			socket.create_connection((public_ip,22),timeout=60)
			print "Sleeping 10 seconds after timeout"
			sleep(10)
			socket.create_connection((public_ip,22),timeout=60)
			print "Sleeping 10 seconds after timeout"
			sleep(10)
		except Exception, e:
			print "Sleeping 10 seconds on socket connection error."
			sleep(10)
			continue
		can_ssh = True

	
	# prepare for local openvpn configuration
	if not os.path.exists("openvpn_client"):
		os.makedirs("openvpn_client")
	#fabric config	
	with settings(host_string=public_ip,user='ubuntu',key_filename=ssh_key,reject_unknown_hosts=False,disable_known_hosts=True,skip_bad_hosts=True,no_agent=True):
		# now run the actual install steps
		sudo("apt-get -y install openvpn easy-rsa")
		# copy openvpn to local root directory
		run("mkdir myopenvpn")
		run("cp -r /usr/share/easy-rsa/* myopenvpn/")
		run("cd ~/myopenvpn; source ./vars; ./clean-all; ./pkitool --initca; ./build-dh; ./pkitool --server server; ./pkitool client")
		sudo("mkdir /etc/openvpn/server")
		sudo("mv /home/ubuntu/myopenvpn/keys/server.crt /etc/openvpn/server/")
		sudo("mv /home/ubuntu/myopenvpn/keys/server.csr /etc/openvpn/server/")
		sudo("mv /home/ubuntu/myopenvpn/keys/server.key /etc/openvpn/server/")
		sudo("mv /home/ubuntu/myopenvpn/keys/dh2048.pem /etc/openvpn/server/")
		sudo("cp /home/ubuntu/myopenvpn/keys/ca.crt /etc/openvpn/server/")
		put("server.conf","/home/ubuntu/server.conf")
		sudo("mv /home/ubuntu/server.conf /etc/openvpn/server.conf")
		sudo("/etc/init.d/openvpn restart")
		get("/home/ubuntu/myopenvpn/keys/client.crt","openvpn_client")
		get("/home/ubuntu/myopenvpn/keys/client.key","openvpn_client")
		get("/home/ubuntu/myopenvpn/keys/ca.crt","openvpn_client")
	print
	print 'Done seting up OpenVPN server for the VPC.'
	print 'You can use %s for the OpenVPN client configuration together with the files downloaded in ./openvpn_client/.' % public_ip
	print 'Your subnet is %s, your OpenVPN server on the subnet is %s (this is the same host with the public IP: %s)' % (subnet_arg,private_ip,public_ip)
	print 'When you connect to the OpenVPN, your personal machine will be on the 10.8.0.0/24 subnet and its IP is assigned automatically.'
	print 'All your spark nodes will be on the same %s subnet, starting with master at subnet IP 100, slaves start at 101. Enjoy!' % subnet_arg

	print
	print 'Launching Spark machines in cluster.'
	ami_to_use = default_conda_ami
	
	if hdfs_config:
		ami_to_use = ### ENTER AMI HERE
