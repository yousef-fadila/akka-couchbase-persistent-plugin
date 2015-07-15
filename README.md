# akka-couchbase-persistence-plugin 
This is a couchbase implementation to akka persistence module. 


version 1.0 

The plugin is ready-to-use

Road map: 
1) use couchbase append feature for atomic updating ( now we assure atomic operations in plugin layer, each write is also a read with CAS feature.) This change should dramatically improve performance in writing journals . 


#Licence
This software is licensed under the Apache 2 license: http://www.apache.org/licenses/LICENSE-2.0
