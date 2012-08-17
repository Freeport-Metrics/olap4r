require "spec_helper"

shared_examples_for "an Olap::Connection driver" do
  context "given an instance" do
    context "#cubes" do
      it "returns an array of cubes" do
        connection.cubes.should =~ [
          { :unique_name => "[Sales Ragged]", :name => "Sales Ragged" },
          { :unique_name => "[Warehouse]", :name => "Warehouse" },
          { :unique_name => "[HR]", :name => "HR" },
          { :unique_name => "[Warehouse and Sales]", :name => "Warehouse and Sales" },
          { :unique_name => "[Sales 2]", :name => "Sales 2" },
          { :unique_name => "[Store]", :name => "Store" },
          { :unique_name => "[Sales]", :name => "Sales" }
        ]
      end
    end

    context "#measures" do
      it "returns an array of measures" do
        connection.measures("[HR]").should =~ [
          { :unique_name => "[Measures].[Org Salary]", :name => "Org Salary" },
          { :unique_name => "[Measures].[Count]", :name => "Count" },
          { :unique_name => "[Measures].[Number of Employees]", :name => "Number of Employees" },
          { :unique_name => "[Measures].[Employee Salary]", :name => "Employee Salary" },
          { :unique_name => "[Measures].[Avg Salary]", :name => "Avg Salary" }
        ]
      end
    end

    context "#dimensions" do
      it "returns an array of dimensions" do
        connection.dimensions("[Sales 2]").length.should == 4
      end

      it "returns dimensions with type" do
        dimensions = connection.dimensions "[Sales 2]"
        dimensions[0][:type].should == :measure
        dimensions[1][:type].should == :time
        dimensions[2][:type].should == :other
        dimensions[3][:type].should == :other
      end
    end

    context "#children_lookup" do
      it "returns an array of dimensions for null member" do
        connection.children_lookup("[Sales Ragged]").should =~ [
          { :unique_name => "[Measures]", :name => "Measures", :children => true, :type => :measure },
          { :unique_name => "[Store]", :name => "Store", :children => true, :type => :other },
          { :unique_name => "[Geography]", :name => "Geography", :children => true, :type => :other },
          { :unique_name => "[Store Size in SQFT]", :name => "Store Size in SQFT", :children => true, :type => :other },
          { :unique_name => "[Store Type]", :name => "Store Type", :children => true, :type => :other },
          { :unique_name => "[Time]", :name => "Time", :children => true, :type => :time },
          { :unique_name => "[Product]", :name => "Product", :children => true, :type => :other },
          { :unique_name => "[Promotion Media]", :name => "Promotion Media", :children => true, :type => :other },
          { :unique_name => "[Promotions]", :name => "Promotions", :children => true, :type => :other },
          { :unique_name => "[Customers]", :name => "Customers", :children => true, :type => :other },
          { :unique_name => "[Education Level]", :name => "Education Level", :children => true, :type => :other },
          { :unique_name => "[Gender]", :name => "Gender", :children => true, :type => :other },
          { :unique_name => "[Marital Status]", :name => "Marital Status", :children => true, :type => :other },
          { :unique_name => "[Yearly Income]", :name => "Yearly Income", :children => true, :type => :other }
        ]
      end

      it "returns an array of dimensions for a given first member in hierarchy" do
        connection.children_lookup("[Sales Ragged]", "[Store]").should =~ [
          { :unique_name => "[Store].[All Stores]", :name => "All Stores", :children => true }
        ]
      end

      it "returns an array of children for a given member in hierarchy" do
        connection.children_lookup("[Sales Ragged]", "[Store].[All Stores].[USA].[CA]").should =~ [
          { :unique_name => "[Store].[USA].[CA].[Alameda]", :name => "Alameda", :children => true },
          { :unique_name => "[Store].[USA].[CA].[Beverly Hills]", :name => "Beverly Hills", :children => true },
          { :unique_name => "[Store].[USA].[CA].[Los Angeles]", :name => "Los Angeles", :children => true },
          { :unique_name => "[Store].[USA].[CA].[San Francisco]", :name => "San Francisco", :children => true }
        ]
      end

      it "returns an array of children for a given measures member in hierarchy" do
        connection.children_lookup("[Sales Ragged]", "[Measures]").should =~ [
          { :unique_name => "[Measures].[Unit Sales]", :name => "Unit Sales", :children => false },
          { :unique_name => "[Measures].[Store Cost]", :name => "Store Cost", :children => false },
          { :unique_name => "[Measures].[Store Sales]", :name => "Store Sales", :children => false },
          { :unique_name => "[Measures].[Sales Count]", :name => "Sales Count", :children => false },
          { :unique_name => "[Measures].[Customer Count]", :name => "Customer Count", :children => false }
        ]
      end

      it "recursively returns an array of children for a given dimension" do
        dimension = connection.children_lookup "[Sales Ragged]", "[Time]", true
        dimension[0][:children].should be_an(Array)
        dimension[0][:children][0][:children].should be_an(Array)
        dimension[0][:children][0][:children][0][:children].should be_an(Array)
      end
    end

    context "#execute" do
      let(:cellset) { connection.execute "SELECT [Measures].[Unit Sales] ON COLUMNS, [Store] ON ROWS FROM [Sales]" }

      it "returns Olap::CellSet instance for successful queries" do
        cellset.should be_a(Olap::CellSet)
      end

      it "returns cellset axes" do
        cellset.axes.should =~ [
          { :axis => :columns, :values => [
            [{ :name => "Unit Sales", :unique_name => "[Measures].[Unit Sales]", :drillable => false }]
          ] },
          { :axis => :rows, :values => [
            [{ :name => "All Stores", :unique_name => "[Store].[All Stores]", :drillable => true }]
          ] }
        ]
      end

      it "returns cellset formatted values" do
        cellset.values.should =~ [["266,773"]]
      end

      it "returns cellset raw values" do
        cellset.values(:value).should =~ [[266773.0]]
      end
    end

    context "#execute drill-down query" do
      let(:cellset) { connection.execute %{
        SELECT {[Measures].[Unit Sales], [Measures].[Store Cost], [Measures].[Store Sales]} ON COLUMNS,
          HIERARCHIZE(UNION(CROSSJOIN({[Promotion Media].[All Media]}, {[Product].[All Products]}), CROSSJOIN({[Promotion Media].[All Media]}, [Product].[All Products].Children))) ON ROWS
        FROM [Sales]
        WHERE [Time].[1997]
      } }

      it "returns cellset axes" do
        cellset.axes.should =~ [
          {
            :axis => :columns,
            :values => [
              [{ :name => "Unit Sales", :unique_name => "[Measures].[Unit Sales]", :drillable => false }],
              [{ :name => "Store Cost", :unique_name => "[Measures].[Store Cost]", :drillable => false }],
              [{ :name => "Store Sales", :unique_name => "[Measures].[Store Sales]", :drillable => false}
            ]]
          },
          {
            :axis => :rows, :values => [
              [
                { :name => "All Media", :unique_name => "[Promotion Media].[All Media]", :drillable => true},
                { :name => "All Products", :unique_name => "[Product].[All Products]", :drillable => true }
              ], [
                { :name => "All Media", :unique_name => "[Promotion Media].[All Media]", :drillable => true },
                { :name => "Drink", :unique_name => "[Product].[Drink]", :drillable => true }
              ], [
                { :name => "All Media", :unique_name => "[Promotion Media].[All Media]", :drillable => true },
                { :name => "Food", :unique_name => "[Product].[Food]", :drillable => true}
              ], [
                { :name => "All Media", :unique_name => "[Promotion Media].[All Media]", :drillable => true },
                { :name => "Non-Consumable", :unique_name => "[Product].[Non-Consumable]", :drillable => true }
              ]
            ]
          }
        ]
      end
    end
  end
end

shared_examples_for "an Olap::Connection driver with drillthrough capabilities" do
  context "given an instance" do
    context "#drillthrough" do
      let(:rowset) { connection.drillthrough "DRILLTHROUGH SELECT [Measures].[Unit Sales] ON COLUMNS, [Store] ON ROWS FROM [Sales]" }

      it "returns Olap::RowSet instance for successful queries" do
        rowset.should be_a(Olap::RowSet)
      end

      it "returns rawset axes" do
        rowset.columns.should include(
          { :id => "the_year", :name => "Year" },
          { :id => "unit_sales", :name => "Unit Sales" }
        )
      end

      it "returns rawset values" do
        rowset.values.length.should == 86837
      end
    end
  end
end

describe Olap::Connection, "for invalid connection string" do
  context "#initialize" do
    it "raises an Exception for 'jdbc' connection string" do
      lambda {
        connection = Olap::Connection.new "jdbc"
      }.should raise_error(Olap::InvalidConnectionStringException)
    end

    it "raised an Exception for 'jdbc:invalid:foo' connection string" do
      lambda {
        connection = Olap::Connection.new "jdbc:invalid:foo"
      }.should raise_error(Olap::InvalidOlapDriverException)
    end
  end
end

describe Olap::Connection, "for Mondrian driver" do
   context "#initialize" do
     it "returns Olap::Connection instance for successful connection" do
       connection = Olap::Connection.new RSPEC_CONFIG["mondrian"]["connection_string"]
       connection.should be_a(Olap::Connection)
     end
   end

  context "given an instance" do
    let(:connection) { Olap::Connection.new RSPEC_CONFIG["mondrian"]["connection_string"] }
    it_should_behave_like "an Olap::Connection driver"
     it_should_behave_like "an Olap::Connection driver with drillthrough capabilities"
  end
end

describe Olap::Connection, "for XML/A driver" do
  context "#initialize" do
    it "returns Olap::Connection instance for successful connection" do
      connection = Olap::Connection.new RSPEC_CONFIG["xmla"]["connection_string"]
      connection.should be_a(Olap::Connection)
    end
  end

  context "given an instance" do
    let(:connection) { Olap::Connection.new RSPEC_CONFIG["xmla"]["connection_string"] }
    it_should_behave_like "an Olap::Connection driver"
  end
end
