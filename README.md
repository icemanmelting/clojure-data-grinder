# clojure-data-grinder

The data grinder, as the name might indicate, is built to process and "grind" data. 
Receive data from anywhere, process it, and store it however and wherever you want. 

# Usage

## Summary

The grinder uses .edn files as the main way to configure a pipeline. 

A simple pipeline may be composed by 4 things:

- A source;
- A grinder;
- An enhancer;
- A sink;

The source is part of the pipeline that retrieves the data to be processed;
The grinder is responsible for processing the data and output the data in the format that is required;
The enhancer is used to enrich processed or collected data with cached data from another source, this source
might be a DB, a file, etc;
The sink is responsible for outputting the processed data, be it a DB or a cloud platform, etc.

It is not mandatory although, to have a grinder, you can easily save the data from the source directly with a sink,
if that is what is needed for that specific data source.

## Configuration
```
{:api-server #profile {:default {:port 8080}
                       :staging {}
                       :production {}}
 :steps #profile {:default {:channels [{:name "line-interpreter"
                                        :buffer-size 10}
                                       {:name "line-grinder"
                                        :buffer-size 10}
                                       {:name "line-sink"
                                        :buffer-size 10}]
                            :sources [{:name "file-reader"
                                       :conf {:watch-dir "/mnt/c/Users/iCeMan/Desktop/"}
                                       :v-fn nil
                                       :out ["line-interpreter"]
                                       :type clojure-data-grinder-core.core/map->FileWatcherSource
                                       :poll-frequency-s 5
                                       :threads 1}]
                            :grinders [{:name "line-interpreter"
                                        :conf {}
                                        :v-fn nil
                                        :in "line-interpreter"
                                        :out ["line-grinder"
                                              ;"line-grinder-2"
                                              ]
                                        :fn cdg-test.core/process-file
                                        :poll-frequency-s 5
                                        :threads 1}
                                       {:name "line-grinder"
                                        :conf {}
                                        :v-fn nil
                                        :in "line-grinder"
                                        :out ["line-sink"]
                                        :fn cdg-test.core/grinder-multiplier
                                        :poll-frequency-s 5
                                        :threads 1}]
                            :sinks [{:name "line-sink"
                                     :in "line-sink"
                                     :conf {}
                                     :v-fn nil
                                     :fn cdg-test.core/sink-value
                                     :poll-frequency-s 5
                                     :threads 1}]}
                  :staging {}
                  :production {}}}
```

The current version of the Grinder, contains 2 main configuration points:

- API Server;
- Steps;

The api server contains all the information for the API part of the grinder. 
This provides the user about how many errors have occurred on specific steps, 
or a general view of the entire pipeline.

The steps, are further subdivided into 4 categories:

- Channels;
- Sources;
- Grinders;
- Enhancers;
- Sinks.

The channels contain all the configurations for the channels to be used in the communication between all steps.
The sources contain all the configurations for all the sources present in the pipeline.
The Grinders contain all the configurations for all the grinders present in the pipeline.
The Enhancers contain all the configurations for all the enahancers present in the pipeline.
The Sinks contain all the configurations for all the enahancers present in the pipeline.

 