require "java"
require "olap4j"

java_import "java.sql.Connection"
java_import "java.sql.DriverManager"
java_import "org.olap4j.metadata.Property"
java_import "org.olap4j.OlapConnection"
java_import "org.olap4j.OlapDatabaseMetaData"
java_import "org.olap4j.transform.StandardTransformLibrary"

module Olap #:nodoc:
end

require "olap4r/connection"
require "olap4r/cellset"
require "olap4r/rowset"
require "olap4r/query_builder"