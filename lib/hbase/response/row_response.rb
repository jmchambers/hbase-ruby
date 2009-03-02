module HBase
  module Response
    class RowResponse < BasicResponse
      def parse_content(raw_data)
        doc = REXML::Document.new(raw_data)
        row = doc.elements["row"]
        columns = []
        count = 0
        row.elements["columns"].each do |col|
          name = col.elements["name"].text.strip.unpack("m").first
          value = col.elements["value"].text.strip.unpack("m").first rescue nil
          timestamp = col.elements["timestamp"].text.strip.to_i
          columns << Model::Column.new(:name => name,
                                       :value => value,
                                       :timestamp => timestamp)
          count += 1
        end
        Model::Row.new(:total_count => count, :columns => columns)
      end
    end
  end
end
