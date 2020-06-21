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
  
