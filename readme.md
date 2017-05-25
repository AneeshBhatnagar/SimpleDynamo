# Simple Amazon Dynamo Style Storage in Android
A simple Amazon Dynamo Style storage built using the Chain Replication method to support insertions, deletions, querying all concurrently and even under one failure. Built as a part of the CSE 586 Distributed Computers course at University at Buffalo.

### Usage
* Create the 2 AVDs using the create_avd.py script: python2.7 create_avd.py 2 /PATH_TO_SDK/
* Run the 2 AVDs using the run_avd.py script: python2.7 run_avd.py 2
* Run the port redirection script using: python2.7 set_redir.py 10000
* Now, run the grading script as follows: 
./dynamo-grading.linux /path/to/sdk

For more information about the grading script, run ./dynamo-grading.linux -h