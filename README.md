Java PostgreSQL Server (jpgsql)
=============================

Java PostgreSQL Server (shortened in jpgsql) is a library which
provides a Java implementation of the server side PostgreSQL binary
protocol.

This library makes easy to create a server which exposes an interface
which is binary compatible with PostgreSQL.
This means that you can either mock a PostgreSQL database for test
purposes; or write a custom database which can be easily accessed by
any language which has a connector to PostgreSQL.

The easiest way to use this library is to implement an interface which
accepts the query string and returns the resulting rows.

This library provides also support classes to listen the network
socket and spawn a new thread for every incoming connection.

In order to have a complete control over the smallest details, it is
possible to extend the base class which implements the protocol and
give access to the lower level PostgreSQL API.

Please, refer to the Javadoc for more information.

jpgsql is released under the terms of MIT license.

Acknowledgment
--------------

This library has been written as part of a bigger project developed by
the research team of Professor Elisa Bertino at the [Department of
Computer Science of Purdue University](https://www.cs.purdue.edu/).
