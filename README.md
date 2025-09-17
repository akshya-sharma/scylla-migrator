# ScyllaDB Migrator

The ScyllaDB Migrator is a Spark application that migrates data to ScyllaDB from CQL-compatible or DynamoDB-compatible databases.

## Know before you use this fork

The repo is a fork of the original repo provided by ScyllaDB at https://github.com/scylladb/scylla-migrator/releases/tag/v1.1.0

It fixes following issues when using the migrator utility with DynamoDB as the source and Scylladb with alternator API as target.

1. Deleting complete record for delete operation. Original code deletes the record leaving the primary-key and the additional column _dynamo_op_type behind.
2. During update making sure additional column _dynamo_op_type is not added during the update operation.
3. Handling of serialization and deserialization of binary types for both migration and validation. Original code was dropping the binary type values during reshuffle step.

This software is provided as-is, without warranty or representation for any use or purpose.

## Documentation

See https://migrator.docs.scylladb.com.

## Building

To test a custom version of the migrator that has not been [released](https://github.com/scylladb/scylla-migrator/releases), you can build it yourself by cloning this Git repository and following the steps below:

1. Make sure the Java 8+ JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the
   JDK installation.
3. Run `build.sh`.
4. This will produce the .jar file to use in the `spark-submit` command at path `migrator/target/scala-2.13/scylla-migrator-assembly.jar`.

## Contributing

Please refer to the file [CONTRIBUTING.md](/CONTRIBUTING.md).

