require "spec_helper"

describe Olap::QueryBuilder do
  context "#initialize" do
    it "returns Olap::QueryBuilder instance" do
      instance = Olap::QueryBuilder.new
      instance.should be_a(Olap::QueryBuilder)
    end
  end

  context "given an instance" do
    let(:query_builder) { Olap::QueryBuilder.new }

    context "#select with array" do
      it "ignores empty array of columns" do
        query_builder.select :columns, []
        query_builder.to_s.should == ""
      end

      it "builds select :columns" do
        query_builder.select :columns, "[Store].[All Stores]"
        query_builder.to_s.should == "SELECT { [Store].[All Stores] } ON COLUMNS"
      end

      it "builds select :columns with multiple arguments" do
        query_builder.select :columns, "[Store].[All Stores]", "[Store].[All Stores].CHILDREN"
        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores], [Store].[All Stores].CHILDREN)) ON COLUMNS"
      end

      it "ignores empty array of rows" do
        query_builder.select :rows, []
        query_builder.to_s.should == ""
      end

      it "builds select :rows" do
        query_builder.select :rows, "[Store].[All Stores]"
        query_builder.to_s.should == "SELECT { [Store].[All Stores] } ON ROWS"
      end

      it "accepts select :rows with multiple arguments" do
        query_builder.select :rows, "[Store].[All Stores]", "[Store].[All Stores].CHILDREN"
        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores], [Store].[All Stores].CHILDREN)) ON ROWS"
      end

      it "builds select :rows and :column" do
        query_builder.select :rows, "[Store].[All Stores]"
        query_builder.select :columns, "[Measures].[Unit Sales]"
        query_builder.to_s.should == "SELECT { [Measures].[Unit Sales] } ON COLUMNS, { [Store].[All Stores] } ON ROWS"
      end
    end

    context "#select with hashes" do
      it "ignores empty array of columns" do
        query_builder.select :columns, []
        query_builder.to_s.should == ""
      end

      it "builds select :columns" do
        query_builder.select :columns, { :id => "[Store].[All Stores]", :properties => [] }
        query_builder.to_s.should == "SELECT { [Store].[All Stores] } ON COLUMNS"
      end

      it "builds select :columns with multiple arguments" do
        query_builder.select :columns, { :id => "[Store].[All Stores]", :properties => [] }, { :id => "[Store].[All Stores]", :properties => [] }
        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores], [Store].[All Stores])) ON COLUMNS"
      end

      it "ignores empty array of rows" do
        query_builder.select :rows, []
        query_builder.to_s.should == ""
      end

      it "builds select :rows" do
        query_builder.select :rows, { :id => "[Store].[All Stores]", :properties => [] }
        query_builder.to_s.should == "SELECT { [Store].[All Stores] } ON ROWS"
      end

      it "accepts select :rows with multiple arguments" do
        query_builder.select :rows, { :id => "[Store].[All Stores]", :properties => [] }, { :id => "[Store].[All Stores]", :properties => ["children"] }
        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores], [Store].[All Stores].CHILDREN)) ON ROWS"
      end

      it "builds select :rows and :column" do
        query_builder.select :rows, { :id => "[Store].[All Stores]", :properties => [] }
        query_builder.select :columns, { :id => "[Measures].[Unit Sales]", :properties => [] }
        query_builder.to_s.should == "SELECT { [Measures].[Unit Sales] } ON COLUMNS, { [Store].[All Stores] } ON ROWS"
      end

      it "silently ignores :drilldownlevel on root members" do
        query_builder.select :rows, { :id => "[Geography]", :properties => ["drilldownlevel"] }
        query_builder.to_s.should == "SELECT { [Geography] } ON ROWS"
      end
    end

    it "selects the cube" do
      query_builder.from "[Sales]"
      query_builder.to_s.should == "FROM [Sales]"
    end

    it "ignores empty array of conditions" do
      query_builder.where []
      query_builder.to_s.should == ""
    end

    it "adds where conditions" do
      query_builder.where "[Measures].[Unit Sales]"
      query_builder.to_s.should == "WHERE ( [Measures].[Unit Sales] )"
    end

    it "adds multiple where conditions" do
      query_builder.where "[Measures].[Unit Sales]", "[Store].[All Stores]"
      query_builder.to_s.should == "WHERE ( [Measures].[Unit Sales], [Store].[All Stores] )"
    end

    it "builds the whole query with chaining" do
      query_builder.select(:columns, "[Store].[All Stores]", "[Store].[All Stores].CHILDREN").
        select(:rows, "[Measures].[Unit Sales]", "[Measures].[Sales Count]").
        from("[Sales]").
        where("[Store Type].[All Store Types].[Supermarket]")
      query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores], [Store].[All Stores].CHILDREN)) ON COLUMNS, { [Measures].[Unit Sales], [Measures].[Sales Count] } ON ROWS FROM [Sales] WHERE ( [Store Type].[All Store Types].[Supermarket] )"
    end

    it "executes built query" do
      query_builder.select(:columns, "[Store].[All Stores]", "[Store].[All Stores].CHILDREN").
        select(:rows, "[Measures].[Unit Sales]").
        from("[Sales]").
        where("[Store Type].[All Store Types].[Supermarket]")

      connection = Olap::Connection.new RSPEC_CONFIG["mondrian"]["connection_string"]
      connection.execute(query_builder).should be_a(Olap::CellSet)
    end
  end
end

describe Olap::QueryBuilder, "advanced queries" do
  context "given an instance" do
    let(:query_builder) { Olap::QueryBuilder.new }

    context "for a single dimension" do
      it "adds DRILLDOWNLEVEL function for a member of a dimension" do
        query_builder.select :rows, { :id => "[Store].[All Stores].[USA]", :properties => ["drilldownlevel"] }
        query_builder.to_s.should == "SELECT { DRILLDOWNLEVEL([Store].[All Stores].[USA]) } ON ROWS"
      end

      it "adds CHILDREN function for a member of a dimension" do
        query_builder.select :rows, { :id => "[Store].[All Stores].[USA]", :properties => ["children"] }
        query_builder.to_s.should == "SELECT { [Store].[All Stores].[USA].CHILDREN } ON ROWS"
      end

      it "adds HIERARCHIZED and UNION to a set and dimension" do
        query_builder.select :rows,
          "[Geography]",
          { :id => "[Geography].[All Geographys]", :properties => ["children"] }

        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION({ [Geography] }, [Geography].[All Geographys].CHILDREN)) ON ROWS"
      end

      it "adds HIERARCHIZE and UNION for 2 members of a dimension" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel])) ON ROWS"
      end

      it "adds HIERARCHIZE, UNION and member functions for 2 members of a dimension" do
        query_builder.select :rows,
          { :id => "[Store].[All Stores]", :properties => ["children"] },
          { :id => "[Store].[All Stores]", :properties => ["drilldownlevel"] }

        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION([Store].[All Stores].CHILDREN, DRILLDOWNLEVEL([Store].[All Stores]))) ON ROWS"
      end

      it "adds HIERARCHIZE AND 2 UNIONs for 3 members of a dimension" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store].[All Stores].[Canada]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), [Store].[All Stores].[Canada])) ON ROWS"
      end

      it "adds HIERARCHIZE AND 4 UNIONs for 5 members of a dimension" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store].[All Stores].[Canada]",
          "[Store].[All Stores].[Mexico]",
          "[Store].[All Stores].[Vatican]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(UNION(UNION(UNION(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), [Store].[All Stores].[Canada]), [Store].[All Stores].[Mexico]), [Store].[All Stores].[Vatican])) ON ROWS"
      end
    end

    context "for multiple dimensions" do
      it "adds CROSSJOIN for 2 members of 2 dimensions" do
        query_builder.select :rows,
          "[Store]",
          "[Store Type]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN({ [Store] }, { [Store Type] })) ON ROWS"
      end

      it "adds HIERARCHIZE, CROSSJOIN and UNION for 3 members of 2 dimensions" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store Type]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), { [Store Type] })) ON ROWS"
      end

      it "adds HIERACHIZE, CROSSJOIN and 2 nested JOINs for 4 members of 2 dimensions" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store].[All Stores].[Canada]",
          "[Store Type]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN(UNION(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), [Store].[All Stores].[Canada]), { [Store Type] })) ON ROWS"
      end

      it "adds HIERACHIZE, CROSSJOIN and 3 nested JOINs for 5 members of 2 dimensions" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store].[All Stores].[Canada]",
          "[Store Type].[All Store Types].[Deluxe Supermarket]",
          "[Store Type].[All Store Types].[HeadQuarters]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN(UNION(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), [Store].[All Stores].[Canada]), UNION([Store Type].[All Store Types].[Deluxe Supermarket], [Store Type].[All Store Types].[HeadQuarters]))) ON ROWS"
      end

      it "adds HIERACHIZE, CROSSJOIN and 4 nested JOINs for 6 members of 2 dimensions" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store].[All Stores].[Canada]",
          "[Store Type].[All Store Types].[Deluxe Supermarket]",
          "[Store Type].[All Store Types].[HeadQuarters]",
          "[Store Type].[All Store Types].[Gourmet Supermarket]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN(UNION(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), [Store].[All Stores].[Canada]), UNION(UNION([Store Type].[All Store Types].[Deluxe Supermarket], [Store Type].[All Store Types].[HeadQuarters]), [Store Type].[All Store Types].[Gourmet Supermarket]))) ON ROWS"
      end

      it "adds HIERACHIZE, 2 nested CROSSJOINs and 3 nested JOINs for 6 members of 3 dimensions" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          "[Store].[All Stores].[Israel]",
          "[Store Type].[All Store Types].[Deluxe Supermarket]",
          "[Store Type].[All Store Types].[HeadQuarters]",
          "[Product].[All Products].[Food]",
          "[Product].[All Products].[Drink]"

        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN(CROSSJOIN(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel]), UNION([Store Type].[All Store Types].[Deluxe Supermarket], [Store Type].[All Store Types].[HeadQuarters])), UNION([Product].[All Products].[Food], [Product].[All Products].[Drink]))) ON ROWS"
      end

      it "adds HIERACHIZE, 2 nested CROSSJOINs and 3 nested JOINs for 6 members of 3 dimensions with according functions" do
        query_builder.select :rows,
          "[Store].[All Stores].[USA]",
          { :id => "[Store].[All Stores].[Israel]", :properties => ["children"] },
          "[Store Type].[All Store Types].[Deluxe Supermarket]",
          "[Store Type].[All Store Types].[HeadQuarters]",
          "[Product].[All Products].[Food]",
          { :id => "[Product].[All Products].[Drink]", :properties => ["drilldownlevel"] }
        query_builder.to_s.should == "SELECT HIERARCHIZE(CROSSJOIN(CROSSJOIN(UNION([Store].[All Stores].[USA], [Store].[All Stores].[Israel].CHILDREN), UNION([Store Type].[All Store Types].[Deluxe Supermarket], [Store Type].[All Store Types].[HeadQuarters])), UNION([Product].[All Products].[Food], DRILLDOWNLEVEL([Product].[All Products].[Drink])))) ON ROWS"
      end
    end
  end
end