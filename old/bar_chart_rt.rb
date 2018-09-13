require 'pg'
require 'date'
require 'json'
require 'open-uri'
require './config'
require './util'

puts 'Saving to database...'
conn = PG::connect(DB_HOST, 5432, '', '', DB_NAME, DB_USER, DB_PASSWORD)

get_codes().each_slice(100) do |code|
  symbols = code.join(',').gsub(' ','')
  result = JSON.parse open("https://marketdata.websol.barchart.com/getQuote.json?apikey=#{BARCHART_API_KEY}&symbols=#{symbols}&mode=r&jerq=false").read

  result['results'].each do |quote|
    date = Date.parse(quote['tradeTimestamp'])
    ticker = quote['symbol'] 
    open = quote['open']
    high = quote['high']
    low = quote['low']
    close = quote['lastPrice']
    volume = quote['volume']
    dividend = 0
    split = 1
    adj_open = open
    adj_high = high
		adj_low = low
		adj_close = close
		adj_volume = volume

    next if close.to_s.empty?
    
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
    puts ticker
    puts query
puts '--------'
		res = conn.exec(query)
  end
end
