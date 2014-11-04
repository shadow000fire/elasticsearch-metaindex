call mvn package
echo hello
copy /Y target\elastic-ioi-0.0.1-SNAPSHOT.jar C:\Tools\elasticsearch-1.2.1\plugins\ioi
