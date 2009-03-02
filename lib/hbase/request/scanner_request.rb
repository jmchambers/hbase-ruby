module HBase
  module Request
    class ScannerRequest < BasicRequest
      attr_reader :table_name

      def initialize(table_name)
        @table_name = CGI.escape(table_name)
        path = "/#{@table_name}/scanner"
        super(path)
      end

      #TODO: will need to change stop_row to end_row once HBase API updated
      def open(columns, start_row = nil, stop_row = nil, timestamp = nil)
        search = []
        search << pack_params(columns)
        search << "start_row=#{CGI.escape(start_row)}" if start_row
        search << "stop_row=#{CGI.escape(stop_row)}" if stop_row
        search << "timestamp=#{CGI.escape(timestamp)}" if timestamp

        @path << "?" << search.join('&')
      end

      def get_rows(scanner_id, limit = 1)
        @path << "/#{scanner_id}?limit=#{limit}"
      end

      def close(scanner_id)
        @path << "/#{scanner_id}"
      end
    end
  end
end
