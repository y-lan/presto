=============
Release 0.134
=============

General Changes
---------------

* Add cumulative memory statistics tracking and expose the stat in the web interface.
* Remove nullability and partition key flags from :doc:`/sql/show-columns`.
* Remove non-standard ``is_partition_key`` column from ``information_schema.columns``.
* Fix performance regression in creation of ``DictionaryBlock``.

Hive Changes
------------

* The comment for partition keys is now prefixed with *"Partition Key"*.
