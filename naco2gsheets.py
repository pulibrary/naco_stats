#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Push locally collated NACO statistics to Google Drive.
If running manually just `python naco2gsheets.py`
from 20181213
pmg
"""
import ConfigParser
import csv
import glob
import gspread
import httplib2
import os
import logging
import time
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
from shutil import copyfile

http = httplib2.Http()

today = time.strftime('%Y%m%d') # name log files
this_month = time.strftime('%Y%m') 

scopes = 'https://www.googleapis.com/auth/drive.metadata.readonly'

temp_nafprod_file = './naf_prod_temp.csv' # to check all that have been produced
file_location = '/mnt/lib-tsserver/catdiv/NACO/' # with txt files output by macros
log = './logs/'
conf_dir = './conf/'
config = ConfigParser.RawConfigParser()
config.read(conf_dir+'sheet_ids.conf')
online_save_id = config.get('sheets', 'onlinesave')
naf_prod_id = config.get('sheets', 'nafprod')

log_filename = today+'.log' # <= write out values from all naf prod files temporarily
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=log+log_filename,level=logging.INFO)


def get_text(sheet_name):
	'''
	Get values from txt files
	'''
	f1xx = ''
	existing_lines = [] # for dupe detection

	logging.info('get_text')

	# read the Google Sheets. There are two: OnlineSave and NAFProduction
	for line in read_gsheet(sheet_name):
		if sheet_name == 'OnlineSave':
			line = [l.encode('utf8') for l in line][:-2] # encode as utf8 for comparison below + remove last two columns
		elif sheet_name == 'NAFProduction':
			line = [l.encode('utf8') for l in line]
		existing_lines.append(line) # add to list for dupe detection

	# check for dupes
	logging.info('checking for dupes')
	dupe_count = 0
	post_naco_count = 0
	cols = 'D1' # default range in NAFProduction Google Sheet

	if sheet_name == 'NAFProduction':
		for n in glob.glob(file_location+'NAFProduction_*.txt'):
			with open(n,'r') as statsin:
				statsin = csv.reader(statsin, delimiter='\t',quotechar='"', quoting=csv.QUOTE_NONE)
				for row in statsin:
					month_tab = row[1][:-2]
					row = [s.decode('latin1').encode('utf8').strip('"').replace('ß','ǂ') for s in row] # remove quotes

					# write all values to a temp file to check for those that are done within the OnlineSave file
					with open(temp_nafprod_file,'ab+') as tempfile:
						writer = csv.writer(tempfile)
						writer.writerow(row)
						
					if row in existing_lines:
						dupe_count += 1
						pass # because it's already in the google sheet
					else:
						post_naco(sheet_name,month_tab,row,cols)
						post_naco_count += 1

	elif sheet_name == 'OnlineSave':
		naf_prod = [] # 
		with open(temp_nafprod_file,'rb') as temp:
			temp_reader = csv.reader(temp,delimiter=',', quotechar='"')
			for v in temp_reader:
				vgerid = v[0]
				rtype = v[2]
				category = v[3]
				try:
					f1xx = v[4]
				except:
					f1xx = 'NULL' # <= this would indicate a macro error
				relevant_values = [vgerid,category,f1xx]
				naf_prod.append(relevant_values) # a list of everything in NAFProduction files to compare against OnlineSave files, to detect and automatically mark new ones

		# loop through the OnlineSave files in the shared directory
		for o in glob.glob(file_location+'OnlineSave_*.txt'):
			with open(o,'r') as prodin:
				prodin = csv.reader(prodin, delimiter='\t',quotechar='"', quoting=csv.QUOTE_NONE)
				for row in prodin:
					row = [s.decode('latin1').encode('utf8').strip('"').replace('ß','ǂ') for s in row] # remove quotes
					if row in existing_lines:
						dupe_count += 1
						pass # because it's already in the google sheet
					else:
						user = row[1]
						rtype = row[2]
						try:
							f1xx = row[4]
						except:
							f1xx = 'NULL'  # <= this would indicate a macro error
						values_to_test = [user,rtype,f1xx] # see if these values can be marked DONE
						if values_to_test in naf_prod:
							row.append('ROBOT')
							row.append('DONE')
							cols = 'G1' # expand the column range to add the above values
						post_naco(sheet_name,this_month,row,cols)
						post_naco_count += 1
							
	logging.info('%s dupes found in %s' % (dupe_count,sheet_name))
	logging.info('%s new rows added to %s' % (post_naco_count,sheet_name))
	logging.info('=' * 25)
	copyfile(log+log_filename,file_location+'logs/'+log_filename)
	logging.info('log file copied to lib-tsserver')


def post_naco(spreadsheet,month_tab,row,cols):
	"""
	Shows basic usage of the Sheets API.
	Prints values from a sample spreadsheet.
	"""
	# The file token.json stores the user's access and refresh tokens, and is
	# created automatically when the authorization flow completes for the first time.
	spreadsheet_id = ''
	range_ = ''
	store = file.Storage(conf_dir+'token.json')
	creds = store.get()
	if spreadsheet == 'OnlineSave':
		spreadsheet_id = online_save_id
		range_ = 'Sheet1!A1:E1'
	elif spreadsheet == 'NAFProduction': 
		spreadsheet_id = naf_prod_id # this in Team Drive; Tableau refers to a copy by using IMPORTRANGE() within a workbook in 'regular' Drive  
		range_ = month_tab+'!A1:%s' % cols # <= monthly tabs
	value_input_option = 'RAW'
	insert_data_option = 'INSERT_ROWS'
	value_range_body = {
		"values": [
					row
				]
			}
	if not creds or creds.invalid:
		flow = client.flow_from_clientsecrets(conf_dir+'credentials.json', scopes)
		creds = tools.run_flow(flow, store)
	service = build('sheets', 'v4', http=creds.authorize(http),cache_discovery=False)

	# Call the Sheets API
	sheet = service.spreadsheets()

	request = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, insertDataOption=insert_data_option, body=value_range_body)

	response = request.execute()

	print('posting to %s : %s,%s,%s,%s,%s' % (spreadsheet,row[0],row[1],row[2],row[3],row[4])) # just for feedback
	logging.info('%s %s' % (row,response))


def read_gsheet(sheet_name):
	# use creds to create a client to interact with the Google Drive API
	scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
	creds = ServiceAccountCredentials.from_json_keyfile_name(conf_dir+'client_secret.json', scope)
	sheet = ''
	sheet_values = []
	client = gspread.authorize(creds)
	
	# Find a workbook by name and open the first sheet
	# Make sure you use the right name here.
	if sheet_name == 'NAFProduction':
		# TODO?: add new tabs for each month?
		wb = client.open("NAFProduction")
		for tab in wb.worksheets():
			sheet = client.open("NAFProduction").worksheet(tab.title)
			this_sheet_values = sheet.get_all_values()
			for val in this_sheet_values:
				sheet_values.append(val)
	elif sheet_name == 'OnlineSave':
		sheet = client.open("OnlineSave").sheet1
		sheet_values = sheet.get_all_values()

	logging.info('reading Google Sheet %s' % sheet_name)

	return sheet_values


def setup():
	with open(temp_nafprod_file,'wb+') as tempfile:
		tempfile.write("test")
	print('created %s' % temp_nafprod_file)


def cleanup():
	os.remove(temp_nafprod_file)
	print('%s removed' % temp_nafprod_file)


if __name__ == "__main__":
	setup()
	stats = ['NAFProduction','OnlineSave']
	for s in stats:
		get_text(s)
	cleanup()
