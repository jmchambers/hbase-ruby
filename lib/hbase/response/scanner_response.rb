module HBase
  module Response
    class ScannerResponse < BasicResponse
      def parse_content(raw_data)
        doc = REXML::Document.new(raw_data)
        rows = []
        doc.elements["rows"].each do |row|
          row_name = row.elements["name"].text.strip.unpack("m").first
          columns = []
          row.elements["columns"].each do |col|
            name = col.elements["name"].text.strip.unpack("m").first
            value = col.elements["value"].text.strip.unpack("m").first rescue nil
            timestamp = col.elements["timestamp"].text.strip.to_i
            columns << Model::Column.new(:name => name,
                                         :value => value,
                                         :timestamp => timestamp)
          end
          rows << Model::Row.new(:name => row_name, :columns => columns)
        end
        rows
      end

      def get_scanner_id
        location = @raw_data['location']
        paths = location.split('/')
        Model::Scanner.new(:table_name => paths[1], :scanner_id => paths[3])
      end
    end
  end
end
