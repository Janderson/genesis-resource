#!/usr/bin/env python
import urllib

# URL and query string
URL = 'http://pxweb.dtn.com/PXWebSvc/PXServiceWeb.svc'
QUERY= 'GetQuoteSnap?UserID=user&Password=pswd&Type=F&Symbol=MSFT,UIS,@C`## 5,IBM'

# Build and send the request
request = '%s/%s' % ( URL, urllib.quote( QUERY, '?&=' ) )
web = urllib.urlopen( request )

# Get the response and print as a string
response = web.read()
print(response)