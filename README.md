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

##API Server

The api server contains all the information for the API part of the grinder. 
This provides the user about how many errors have occurred on specific steps, 
or a general view of the entire pipeline.

Its configuration for the moment, is composed only of the port where the server is going to be running.

##Steps

The steps contain everything related to the pipeline, i.e. everything from the pipes (channels) to the steps themselves.
As mentioned above, there are multiple types of steps that can be used in the configuration. These need to be added to 
the respective vector inside the configuration. Therefore we will end up with:

`:sources []` - for all the sources used in the pipeline;
`:grinders []` - for all the grinders used in the pipeline;
`:sinks []` - for all the sinks used in the pipeline;

each type has common arguments with all other steps, as well as custom ones, that are only used in that specific step.

###Sources Configuration

Each source has the following arguments:

- `:name` - the name of the step; 
- `:conf` - a common map that contains the configuration needed for this specific source;
- `:v-fn` - validation function, the function to be used to validate the configuration provided above;
- `:out` - the vector that contains the names of the channels used as the output of this source;
- `:type` - this is used in case we have a predefined source type (an external library for isntance) that was previously
implemented. If this is not present, then it will try to check if a custom function is present to use as a source.
- `:poll-frequency-s` - the poll frequency for the source in seconds. How long should it wait to run the source function again.
- `:threads` - the number of threads used to run this source (default is 1).
- `:fn` - in case type is not present, this will contain the function used as the source. This will only be used if `:type`
IS NOT PRESENT!

###Grinders Configuration

Each Grinder has the following arguments

- `:name` - the name of the step;
- `:conf` - a common map that contains the configuration needed for this specific grinder;
- `:v-fn` - validation function, the function to be used to validate the configuration provided above; 
- `:in` - the name of the channel that provides the data to be grinded;
- `:x-fn` - the transformation function responsible for grinding the data input;
- `:out` - the vector that contains the names of the channels used as the output of this grinder;
- `:poll-frequency-s` - the poll frequency for the grinder in seconds. How long should it wait to read from the input channel again.


