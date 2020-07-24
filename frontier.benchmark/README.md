## Building 
The project follows the standard Maven directory structure, with two
differentiated modules, seep-system and seep-streamsql.

To build meander, you need `maven` and `ant` installed:

```
sudo apt-get update
sudo apt-get install maven ant
```

Next, you need to set the repository directory `repoDir` in the Frontier
configuration file. For example, if your copy of the Frontier
repository is at `/home/myusername/dev/frontier`, then change the value of
the `repoDir` property in
`/home/myusername/dev/frontier/seep-system/src/main/resources/config.properties`
to `repoDir=/home/myusername/dev/frontier`.

Then from the top level directory:

```./frontier-bld.sh core```

## Running
The system requires one master node and N worker nodes (one worker node per
Operator).

First set the IP address of the master node in `mainAddr` inside
`config.properties` and rebuild the system. By default it is `127.0.0.1`
so you don't need to change anything if running in local mode (see below).

To run the master for the stateful window query example:
```
cd seep-system/examples/stateful-window-query
java -jar lib/seep-system-0.0.1-SNAPSHOT.jar Master `pwd`/dist/stateful-window-query.jar Base
```

Run 3 workers in different terminal tabs (1 master + 3 workers):

``` 
cd seep-system/examples/stateful-window-query
java -jar lib/seep-system-0.0.1-SNAPSHOT.jar Worker 3501 2>&1 | grep output-window-lat
java -jar lib/seep-system-0.0.1-SNAPSHOT.jar Worker 3502 2>&1 | grep output-window-lat
java -jar lib/seep-system-0.0.1-SNAPSHOT.jar Worker 3503 2>&1 | grep output-window-lat
```
After that enter '7' in master's tab, it will submit queries to nodes and start processing.
You will see worker output like 'WindowTimestamp: 1593962629792 EventsInWindowCount: 841 Latency: 49 ms.' in the one of the tabs.
## Change event generator rate
To setup event generator rate (events/second) you need to change '/frontier/seep-system/examples/stateful-window-query/src/Base.java' line 23
```
int eventsInSecond = 100; // Declare EVENTS/SEC rate here
```

and rebuild project from the root with :
```./frontier-bld.sh core```

## Build charts

Run python3 plot_results.py in the root folder.

## Semi-implemented solutions
examples/stateful-test <- another one stateful implementation with RangeWindow from seep-streamsql
