require 'quandl'
require 'pg'
require './config'
require 'date'
require './util'

Quandl::ApiConfig.api_key = QUANDL_API_KEY

if ARGV.count != 2 or !(['partial', 'complete'].include? ARGV[1])
  puts 'Usage: ruby quandl_bulk_download.rb <database> <partial/complete>'
  exit
end

puts 'Downloading...'

if ARGV[1] == 'partial'
  file_name = "#{ARGV[0]}.partial"
else
  file_name = ARGV[0]
end

Quandl::Database
  .get(ARGV[0])
  .bulk_download_to_file('.', params: { download_type: ARGV[1] })

puts 'Download completed. Unpacking...'
system "unzip -p #{file_name}.zip > ./data/#{file_name}.csv"

puts 'Saving to database...'
conn = PG::connect(DB_HOST, 5432, '', '', DB_NAME, DB_USER, DB_PASSWORD)

File.open("data/#{file_name}.csv").each do |line|
  column = line.split(',')
  ticker = column[0]
  date = column[1]
  open = zero_empty(column[2])
  high = zero_empty(column[3])
  low = zero_empty(column[4])
  close = zero_empty(column[5])
  volume = zero_empty(column[6])
  dividend = zero_empty(column[7])
  split = zero_empty(column[8])
  adj_open = zero_empty(column[9])
  adj_high = zero_empty(column[10])
  adj_low = zero_empty(column[11])
  adj_close = zero_empty(column[12])
  adj_volume = zero_empty(column[13])
    
    
query = "INSERT INTO eod_us_stock_prices (
          ticker, 
          date, 
          open, 
          high, 
          low, 
          close, 
          volume, 
          dividend, 
          split, 
          adj_open, 
          adj_high, 
          adj_low, 
          adj_close, 
          adj_volume) 
          VALUES (
            '#{ticker}', 
            '#{date}',
            #{open},
            #{high},
            #{low},
            #{close},
            #{volume},
            #{dividend},
            #{split},
            #{adj_open},
            #{adj_high},
            #{adj_low},
            #{adj_close},
            #{adj_volume}
          ) ON CONFLICT(ticker, date) DO UPDATE
          SET open = excluded.open,
              high = excluded.high, 
              low  = excluded.low, 
              close = excluded.close, 
              volume = excluded.volume, 
              dividend = excluded.dividend, 
              split = excluded.split, 
              adj_open = excluded.adj_open, 
              adj_high = excluded.adj_high, 
              adj_low = excluded.adj_low, 
              adj_close = excluded.adj_close, 
              adj_volume = excluded.adj_volume;"

  res = conn.exec(query)
  # print '.'
end

puts "\nQuotes saved."

