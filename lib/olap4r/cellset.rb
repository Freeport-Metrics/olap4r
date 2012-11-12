module Olap
  class CellSet
    def initialize(cellset) # :nodoc:
      @cellset = cellset
    end

    # Returns list of axes
    #
    def axes
      @axes ||= @cellset.get_axes.map do |axis|
        {
          :axis => axis.get_axis_ordinal.to_s.downcase.to_sym,
          :values => axis.get_positions.map do |position|
            position.get_members.inject [] do |members, member|
              members << {
                :name => member.get_caption,
                :unique_name => member.get_unique_name,
                :drillable => member.get_child_member_count > 0
              }
            end
          end
        }
      end
    end

    # Returns query values
    #
    # ==== Attributes
    #
    # * +value_type+ - Returned value type (:value or :formatted_value)
    #
    def values(value_type = :formatted_value)
      return @values unless @values.nil?

      @values = []

      raise "olap4r doesn't support queries with more than 2 dimensions" if @cellset.get_axes.size > 2

      columns = @cellset.get_axes[0].get_positions.size - 1
      rows    = @cellset.get_axes[1].get_positions.size - 1

      cells = []

      (0..columns).each do |i|
        (0..rows).each do |j|
          cells << [i, j]
        end
      end

      cells.each do |cell|
        @values[cell[1]] = [] if @values[cell[1]].nil?
        @values[cell[1]][cell[0]] = @cellset.get_cell(cell.map { |coord| coord.to_java(:int) }).send :"get_#{value_type}"
        @values[cell[1]][cell[0]] = "0" if @values[cell[1]][cell[0]].respond_to?(:empty?) && @values[cell[1]][cell[0]].empty?
      end

      @values
    end
  end
end
