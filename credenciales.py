from oauth2client.service_account import ServiceAccountCredentials
import gspread
json = 'key.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
gc = gspread.authorize(creds)
token='6179451647:AAHhUOphBoMm4oGx_A2eDHJTPfzEW-fbVyE'
key = '1rpT1LlBOKPv547CM4z3Z1Wn7AXF8ebfvPyMcP4McH4k'

