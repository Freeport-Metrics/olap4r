# olap4r

olap4r is a Ruby wrapper for olap4j library. It was open-sourced as a part of [data visualization and reporting tool - Rubbi](http://rubbi.net).

## Examples

Create connection:

    connection = Olap::Connection.new "jdbc:mondrian:JdbcDrivers=com.mysql.jdbc.Driver;Jdbc=jdbc:mysql://localhost/mondrian_foodmart?user=root;Catalog=file:spec/fixtures/FoodMart.xml;"

Execute queries:

    results = connection.execute "SELECT [Measures].[Unit Sales] ON COLUMNS, [Store] ON ROWS FROM [Sales]"
    puts results
    # => [["266,773"]]

Build a query:

    query_builder.select(:columns, "[Store].[All Stores]", "[Store].[All Stores].CHILDREN").
      select(:rows, "[Measures].[Unit Sales]", "[Measures].[Sales Count]").
      from("[Sales]").
      where("[Store Type].[All Store Types].[Supermarket]")
    query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores], [Store].[All Stores].CHILDREN)) ON COLUMNS, { [Measures].[Unit Sales], [Measures].[Sales Count] } ON ROWS FROM [Sales] WHERE ( [Store Type].[All Store Types].[Supermarket] )"

## Releasing new version

Install required development toolset:

    bundle install

Write tests, write code. If you're ready to release a new version __first__ commit your changes, then run:

    rake version:bump:patch

To pull new gem into application switch to the same exact Ruby and gemset that you're using in your application, ie. if you're using `rvm jruby@myapp` for your app then use it to install this gem too.

    bundle install
    rake install

Switch to your application and run:

    bundle update olap4r

## Testing

Configure ``spec/config.yml`` using your JDBC / XML/A details (you'll find example in ``spec/config.yml.example``):

    mondrian:
      jdbc_driver_path: "/usr/local/Cellar/tomcat/7.0.6//libexec/common/endorsed/mysql-connector-java-5.1.15-bin"
      connection_string: "jdbc:mondrian:JdbcDrivers=com.mysql.jdbc.Driver;Jdbc=jdbc:mysql://localhost/mondrian_foodmart?user=root;Catalog=file:spec/fixtures/FoodMart.xml;"

    xmla:
      connection_string: "jdbc:xmla:Server=http://127.0.0.1:8080/mondrian/xmla;Catalog=FoodMart;"

``jdbc_driver_path`` is a path to the JAR file which you need as specified in the ``mondrian`` ``connection_string``.

## Copyright

Copyright 2011-2012 Freeport Metrics Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.