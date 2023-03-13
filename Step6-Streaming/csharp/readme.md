# Kafka Sample Application with .Net Core

This is Kafka sample application using .Net Core. 

- Producer

  The producer sends messages using the Confluent Cloud Kafka service 

- Consumer

  The consumer connect to the Confluent Cloud and consume the messages

## Create the Confluent Cloud Account

 - https://confluent.cloud/signup
 - Add the topic 
     purchases

## Install the .Net Core Runtime and SDK

```
$ wget https://dot.net/v1/dotnet-install.sh 
$ sudo chmod +x ./dotnet-install.sh
$ dotnet-install.sh --version latest 
```

- Permanently add the path to the shell configuration 
```
$ nano ~/.bashrc

# add these lines at the end of the file
export DOTNET_ROOT=$HOME/.dotnet
export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools

```

### Create the producer and consumer projects

This creates two folders and projects

```

$ dotnet new console -a producer -o producer
$ dotnet new console -a consumer -o consumer

```
Add the code to your projects.

### Save the API credentials in the .kafka home folder

```
$ cd ~/
$ mkdir .kafka
$ touch .kafka/confluent.properties

# add the properties file content into the properties file

```

