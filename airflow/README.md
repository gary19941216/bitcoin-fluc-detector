# **How to setup Airflow on a single Ubuntu EC2 instance**

  1.  Install pip3:
  
      ```sudo install python3-pip```
      
  2.  Install Airflow:
  
      ```sudo pip3 install apache-airflow```
      
  3.  Install Hive plugin to Airflow:
  
      ```pip3 install apache-airflow[hive]```
      
  4.  Initialize the database:
  
      ```airflow initdb```
      
  5.  Add ```~/.local/bin``` to PATH:
  
      ```export PATH=$PATH:~/.local/bin```
      
  6.  Start the web server on port 8081:
  
      ```airflow webserver -p 8081```
      
  7.  Configure ```~/aiflow/airflow.cfg``` file:
  
      ```vim ~/aiflow/airflow.cfg```
      
      dags_folder: change to the folder where your python code are located.
      
  8.  Run python code:
  
      ```python3 sparkDAG.py```
      
  9.  Check if your DAG is added into Airflow:
  
      ```airflow list_dags```
      
  10. Test if it work by using backfill:
  
      ```airflow backfill reddit_bitcoin_DAG -s 2020-06-21```
      
  11. Run the DAG with the actual DAG configs:
  
      ```airflow scheduler```
      
