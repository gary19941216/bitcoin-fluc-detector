# **How to setup cassandra on a single Ubuntu EC2 instance** 

  1.  Update apt: 
  
      ```sudo apt update```
      
  2.  Install OpenJDK package:
  
      ```sudo apt install openjdk-8-jdk```
      
  3.  Verify the Java installation:
  
      ```java -version```
      
  4.  Install the apt-transport-https package that is necessary to access a repository over HTTPS:
  
      ```sudo apt install apt-transport-https```
      
  5.  Add the Apache Cassandra repository. Import the repository’s GPG using the following wget command:
  
      ```wget -q -O - https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -```
      
  6.  Add the Cassandra repository to the system:
  
      ```sudo sh -c 'echo "deb http://www.apache.org/dist/cassandra/debian 311x main" > /etc/apt/sources.list.d/cassandra.list' ```
      
  7.  Update the apt package list and install the latest version of Apache Cassandra:
  
      ```sudo apt update``` and ```sudo apt install cassandra```
      
  8.  Verify that Cassandra is running by typing:
  
      ```nodetool status```
      
  9.  If success, something simliar should be shows as below:
  
      ```bash
      Datacenter: datacenter1
      =======================
      Status=Up/Down
      |/ State=Normal/Leaving/Joining/Moving
      --  Address    Load        Tokens       Owns (effective)  Host ID                               Rack
      UN  127.0.0.1  114.55 KiB  256          100.0%            d8c27e24-ea26-4eeb-883c-5986218ba3ca  rack1
      ```
  
  10.  If you're going to setup a cassandra cluster, repeat these steps on all the instances.
  
  
  
# **How to setup cassandra cluster** 

  1.  Suppose we have four EC2 instances, two of them will be the seeds, two of them will be the replica.
  
      ```bash
      10.0.0.5 (seed1)
      10.0.0.6 (seed2)
      10.0.0.12
      10.0.0.14
      ```
  
  2.  Stop Cassandra:
  
      ```sudo service cassandra stop```
      
  3.  Clear the data:
  
      ```sudo rm -rf /var/lib/cassandra/*```
      
  4.  Go to the folder where cassandra config file are located:
  
      ```cd /etc/cassandra/```
      
  5.  configure ```cassandra.yaml``` file:
  
      ```vim cassandra.yaml```
      
      cluster_name: make sure all of the EC2 instances have the same cluster name
      
      - seeds: "10.0.0.5,10.0.0.6"
      
      listen_address: ip of the instance you are currently configuring
      
      rpc_address: 0.0.0.0
      
      broadcast_rpc_address: ip of the instance you are currently configuring
      
      endpoint_snitch: GossipingPropertyFileSnitch
      
  6.  configure ```cassandra-rackdc.properties``` file:
  
      ```bash
      # indicate the rack and dc for this node
      dc=DC1
      rack=RAC1
      ```
      
      We only have four nodes, so setting all of them in same data center and same rack would be sufficient.
      Make sure all of them have the same data center and rack name.
      
  7.  start cassandra:
  
      ```sudo service cassandra start```
      
      Make sure you have done step2 and step3 on every instances before starting.
      If some instances haven't been stopped, those instances will have problem being adding into the cluster.
      
      Start the node one by one.
      Starting from the seed nodes.
      Make sure to start the next one only if the previous one is added into the cluster without any problem.
      Use ```nodetool status``` to check.
      
      
