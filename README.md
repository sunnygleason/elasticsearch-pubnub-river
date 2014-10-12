PubNub River Plugin for ElasticSearch
==================================

The PubNub River plugin allows PubNub content to be indexed by ElasticSearch.

In order to install the plugin, simply run: `bin/plugin -install sunnygleason/elasticsearch-pubnub-river/0.0.1`.

    --------------------------------------
    | PubNub Plugin   | ElasticSearch    |
    --------------------------------------
    | master          | 1.3.4 -> master  |
    --------------------------------------
    | 0.0.1           | 1.3.4 -> master  |
    --------------------------------------

PubNub River allows ElasticSearch to automatically index content via PubNub data stream network.

Creating the PubNub river is as simple as:

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "pubnub",
	    "pubnub" : {
	        "publishKey"   : "demo", 
	        "subscribeKey" : "demo",
	        "useSsl"       : "true",
	        "channels"     : "test_elasticsearch"
	    },
	    "index" : {
	        "bulk_size" : 100
	    }
	}'

The river will automatically bulk index queue messages when the queue is overloaded, allowing for faster
catch-up with content streamed into the queue.
