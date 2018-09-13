def get_codes
	conn = PG::connect(DB_HOST, 5432, '', '', DB_NAME, DB_USER, DB_PASSWORD)

  query = "SELECT DISTINCT(ticker) FROM eod_us_stock_prices"
	res = conn.exec(query)

  res.column_values(0)
end

def zero_empty(price) 
  if price.strip.empty?
    0
  else
    price
  end
end
