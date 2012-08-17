module Olap
  class RowSet
    def initialize rowset
      @rowset = rowset
    end

    # Returns list of columns
    #
    def columns
      @columns ||= 1.upto(@rowset.get_meta_data.get_column_count).map do |i|
        {
          :id => @rowset.get_meta_data.getColumnName(i),
          :name => @rowset.get_meta_data.getColumnLabel(i)
        }
      end
    end

    # Returns query values
    #
    # ==== Attributes
    #
    # * +value_type+ - Returned value type (:value or :formatted_value)
    #
    def values value_type = :formatted_value
      return @values unless @values.nil?

      @values = []
      while @rowset.next do
        @values << 1.upto(self.columns.size).map do |i|
          @rowset.getString i
        end
      end

      @values
    end
  end
end
