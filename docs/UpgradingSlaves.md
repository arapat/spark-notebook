## Upgrading Slaves
Sometimes you need to add or upgrade the software on all of the slave
nodes.

This note gives you directions for how to do that.

### Logging into slaves

In order to figure out what is missing on the slave nodes, it is
useful to log into them. This is done as follows:

1. Use the command given on the pyspark-notebook page to log into the head node:  
   ```ssh -i /Users/yoavfreund/projects/spark-notebook/yfreund_yoav-freunds-PowerBook-G4-retina.local_1483916791.pem ec2-user@ec2-54-89-99-186.compute-1.amazonaws.com```
2. List the IPs of the slave nodes:  
```
cat hadoop/conf/slaves
```  
You will get something like the following list
```
  ec2-54-82-250-252.compute-1.amazonaws.com  
  ec2-54-161-193-44.compute-1.amazonaws.com  
  ec2-52-90-111-132.compute-1.amazonaws.com  
  ec2-52-23-208-230.compute-1.amazonaws.com  
  ec2-54-145-75-189.compute-1.amazonaws.com  
  ec2-54-152-243-179.compute-1.amazonaws.com  
  ec2-54-165-78-113.compute-1.amazonaws.com  
  ec2-54-84-1-233.compute-1.amazonaws.com  
  ec2-54-164-171-93.compute-1.amazonaws.com  
  ec2-54-144-62-76.compute-1.amazonaws.com  
```
3. ssh into one of the slave nodes:  
```ssh ec2-54-165-78-113.compute-1.amazonaws.com```
4. Check the versions of a library  
```
  [ec2-user@ip-10-0-0-72 ~]$ python
  Python 2.7.10 (default, Dec  8 2015, 18:25:23)
  [GCC 4.8.3 20140911 (Red Hat 4.8.3-9)] on linux2
  Type "help", "copyright", "credits" or "license" for more information.
  >>> import numpy
  >>> numpy.version.version
  '1.12.0'
```

### Using flintrock to install packages:
In this directory (the park-notebook root directory) there is a useful command called `flintrock` on which 
the spark-python is based.

You can use this command to install new packages. To do that you use the `run-command` subcommand:

```
flintrock --config ./config.yaml run-command <cluster_name> --ec2-identity-file <path_to_the_ssh_key_for_ec2> <command>
```
where:

* **cluster name** is the name of the cluster as it appears in the notebook management webpage.
* **path_to_the_ssh_key_for_ec2** is the `.pem` file that is defined in the ssh command from the management web page.
* **command** is the unix command you want to execute on all of the nodes.

#### Examples

to install gcc:
```
flintrock --config ./config.yaml run-command Whales-Big --ec2-identity-file /Users/yoavfreund/projects/spark-notebook/yfreund_yoav-freunds-PowerBook-G4-retina.local_1483916791.pem 'sudo yum install -y gcc gcc-c++'
```

to upgrade numpy to the latest version:

```
flintrock --config ./config.yaml run-command Whales-Big --ec2-identity-file /Users/yoavfreund/projects/spark-notebook/yfreund_yoav-freunds-PowerBook-G4-retina.local_1483916791.pem 'sudo pip install --upgrade numpy'
```

