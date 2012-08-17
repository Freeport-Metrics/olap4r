Java::JavaClass.for_name "org.olap4j.mdx.IdentifierNode"

module Olap #:nodoc:
  class InvalidConnectionStringException < Exception; end
  class InvalidOlapDriverException < Exception; end

  class Connection

    # Returns new OLAP connection.
    #
    # ==== Attributes
    #
    # * +connection_string+ - OLAP connection string.
    #
    # ==== Examples
    #
    # To create new connection provide a JDBC connection string:
    #
    #    @olap = Olap::Connection.new "jdbc:mondrian:JdbcDrivers=com.mysql.jdbc.Driver;Jdbc=jdbc:mysql://127.0.0.1/olap?user=olap&password=olap;Catalog=file:/home/olap/schemas/Olap.xml;"
    #
    def initialize(connection_string)
      begin
        driver = connection_string.match(/\Ajdbc\:([A-Za-z]+)\:/)[1]
      rescue NoMethodError
        raise Olap::InvalidConnectionStringException.new
      end

      begin
        raise
        @connection = DriverManager.get_connection connection_string
      rescue
        begin
          driver_class = Olap.const_get("#{driver.capitalize}")
          driver = driver_class.jdbc_driver
          properties = java.util.Properties.new
          @connection = driver_initialize(driver).connect(connection_string, properties)
        rescue NameError => e
          raise Olap::InvalidOlapDriverException.new
        end
      end
    end

    # Executes regular MDX query.
    #
    # ==== Attributes
    #
    # * +query+ - MDX Query.
    #
    def execute(query)
      CellSet.new @connection.create_statement.execute_olap_query(query.to_s)
    end

    # Executes drillthrough MDX query.
    #
    # ==== Attributes
    #
    # * +query+ - MDX Query.
    #
    def drillthrough(query)
      RowSet.new @connection.create_statement.execute_query(query.to_s)
    end

    # Returns list of all cubes.
    #
    def cubes
      @cubes ||= @connection.get_olap_schema.get_cubes.map do |cube|
        {
          :unique_name => cube.get_unique_name,
          :name => cube.get_caption
        }
      end
    end

    # Returns list of all measures for cube.
    #
    # ==== Attributes
    #
    # * +cube_unique_name+ - Cube name
    #
    def measures(cube_unique_name)
      @measures = {} if @measures.nil?

      @measures[cube_unique_name] ||= cube(cube_unique_name).get_measures.map do |measure|
        {
          :unique_name => measure.get_unique_name,
          :name => measure.get_caption
        }
      end
    end

    # Returns list of all dimensions for cube.
    #
    # ==== Attributes
    #
    # * +cube_unique_name+ - Cube name
    #
    def dimensions(cube_unique_name)
      @dimensions = {} if @dimensions.nil?

      @dimensions[cube_unique_name] ||= cube(cube_unique_name).get_dimensions.map do |dimension|
        {
          :unique_name => dimension.get_unique_name,
          :name => dimension.get_caption,
          :children => true,
          :type => dimension.get_dimension_type.to_s.downcase.to_sym
        }
      end
    end

    # Returns list of member children
    #
    # ==== Attributes
    #
    # * +cube_unique_name+ - Cube name
    # * +member+ - Root member element
    # * +recursive+ - Recursive lookup
    #
    def children_lookup(cube_unique_name, member = nil, recursive = false)
      return dimensions cube_unique_name if member.nil?

      if member.split(".").length == 1
        cube(cube_unique_name).get_dimensions.reject { |dimension| dimension.get_unique_name != member }.first.get_hierarchies.map { |hierarchy| hierarchy.get_root_members.map { |member| dimension(member, recursive)} }.flatten
      else
        children cube(cube_unique_name).send(:lookup_member, Java::OrgOlap4jMdx::IdentifierNode.parse_identifier(member).get_segment_list()), recursive
      end
    end


    private


    def driver_initialize(klass)
      constructor = klass.java_class.declared_constructor
      constructor.accessible = true

      begin
        return constructor.new_instance.to_java
      rescue TypeError
        false
      end
    end

    def cube(cube_unique_name)
      @connection.get_olap_schema.get_cubes.reject { |cube| cube.get_unique_name != cube_unique_name }.first
    end

    def dimension(member, recursive = false)
      {
        :unique_name => member.get_unique_name,
        :name => member.get_property_value(Java::org::olap4j::metadata::Property::StandardMemberProperty::MEMBER_CAPTION),
        :children => recursive ? children(member, recursive) : member.get_child_member_count > 0
      }
    end

    def children(member, recursive = false)
      member.get_child_members.inject([]) do |children, root_member|
        children << dimension(root_member, recursive)
      end
    end
  end
end