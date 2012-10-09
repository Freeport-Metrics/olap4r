module Olap
  class QueryBuilder
    def initialize
      @select = {:columns => [], :rows => []}
      @from = nil
      @conditions = []
    end

    def select axis, *fields
      fields.flatten.each { |field| @select[axis] << field_as_string(field) }
      self
    end

    def from cube
      @from = cube
      self
    end

    def where *conditions
      conditions.flatten.each { |condition| @conditions << condition }
      self
    end

    def to_s
      build_query
    end


    private


    def field_as_string field
      if field.is_a?(Hash)
        field_with_properties field[:id], field[:properties].map { |property| property.downcase.to_sym }
      else
        field
      end
    end

    def field_with_properties field, properties
      if properties.include?(:children)
        field = "#{field}.CHILDREN"
      end

      if properties.include?(:drilldownlevel) && level(field) > 1
        field = "DRILLDOWNLEVEL(#{field})"
      end

      field
    end

    def extract_hierarchy(field)
      field.match(/\[([^\]]+)\]/)[0]
    end

    def build_axis axis
      return nil if @select[axis].empty?

      hierarchies = {}
      @select[axis].each do |field|
        hierarchy = extract_hierarchy(field)
        hierarchies[hierarchy] = [] if hierarchies[hierarchy].nil?
        hierarchies[hierarchy].push field
      end

      if hierarchies.keys.length == 1
        if hierarchies.keys[0] == "[Measures]" || @select[axis].length == 1
          "{ #{@select[axis].join ", "} }"
        else
          "HIERARCHIZE(#{build_union @select[axis]})"
        end
      else
        "HIERARCHIZE(#{build_crossjoin hierarchies.map { |hierarchy, fields| build_union(fields) }})"
      end
    end

    def build_field field
      # Case for dimensions - dimensions need to be wrapped as sets
      if field.split(".").length == 1
        "{ #{field} }"
      else
        field
      end
    end

    def build_union fields
      return "{ #{fields.join(", ")} }" if fields.length == 1

      unionized = "UNION(#{build_field fields[0]}, #{build_field fields[1]})"

      2.upto(fields.length - 1) do |i|
        unionized = "UNION(#{unionized}, #{build_field fields[i]})"
      end

      unionized
    end

    def build_crossjoin hierarchy
      crossjoined = "CROSSJOIN(#{hierarchy[0]}, #{hierarchy[1]})"

      2.upto(hierarchy.length - 1) do |i|
        crossjoined = "CROSSJOIN(#{crossjoined}, #{hierarchy[i]})"
      end

      crossjoined
    end

    def build_conditions
      filters_by_hierarchy = @conditions.inject({}) { |hash, c|
        hierarchy = extract_hierarchy(c)
        if !hash[hierarchy] then
          hash[hierarchy] = [c]
        else
          hash[hierarchy] << c
        end
        hash
      }
      filters_by_hierarchy.collect { |k, v| (v.size > 1) ? "{#{v.join(', ')}}" : v }.join(' * ')
    end

    def build_query
      query = []

      columns = build_axis :columns
      rows = build_axis :rows


      unless rows.nil? && columns.nil?
        query << "SELECT"

        fields = []
        fields << "#{columns} ON COLUMNS" unless columns.nil?
        fields << "#{rows} ON ROWS" unless rows.nil?

        query << fields.join(", ")
      end

      query << "FROM #{@from}" unless @from.nil?
      query << "WHERE ( #{build_conditions} )" if @conditions.any?

      query.join " "
    end

    def level field
      field.scan(/\[([^\]]*)\]/).length
    end
  end
end
