from bs4 import BeautifulSoup
import re

def RemoveHTML(content):
	try:
		soup = BeautifulSoup(content, 'html.parser')
		return re.sub("[\\r\\n]+", " ", soup.text)
	except Exception as e:
		raise(e)
 
