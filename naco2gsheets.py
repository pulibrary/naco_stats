#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Push locally collated NACO statistics to Google Drive.
If running manually, adjust the cfg file and run `python naco2gsheets.py`
Requires credentials: https://console.developers.google.com/apis
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
import sqlite3 as lite
import time
from googleapiclient.discovery import build
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
from shutil import copyfile

http = httplib2.Http()

today = time.strftime('%Y%m%d') # name log files
this_year = time.strftime('%Y') 
this_month = time.strftime('%Y%m') 

config = ConfigParser.RawConfigParser()
cwd = os.getcwd()
conf_dir = cwd+'/conf/' # NOTE: this has to be absolute path for cron
config.read(conf_dir+'naco2gsheets.cfg')
temp_nafprod_file = config.get('env', 'temp_nafprof_file') # to check all that have been produced
text_file_location = config.get('env', 'text_files') # with txt files output by macros
log = config.get('env', 'logs') 
online_save_id = config.get('sheets', 'onlinesave')
naf_prod_id = config.get('sheets', 'nafprod')

scopes = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(conf_dir+'client_secret.json', scopes)

log_filename = today+'.log' # <= write out values from all naf prod files temporarily
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=log+log_filename,level=logging.INFO)

def main():
	logging.info('=' * 50)
	logging.info('main()')
	setup()
	stats = ['NAFProduction','OnlineSave']
	for s in stats:
		get_text(s,scopes,creds)
	cleanup()
	logging.info('=' * 50)

def get_text(sheet_name,scopes,creds):
	'''
	Get values from txt files and put them into lists
	'''
	f1xx = ''
	next_row = 0
	existing_lines = [] # for dupe detection
	online_save = []

	logging.info('=' * 25)
	logging.info('= getting data from %s files' % sheet_name)

	# read the Google Sheets. There are two: OnlineSave and NAFProduction
	for line in read_gsheet(sheet_name,scopes,creds):
		line = [l.encode('utf8') for l in line]
		existing_lines.append(line) # add to list for dupe detection

	# check for dupes
	logging.info('= checking for dupes')
	dupe_count = 0
	post_naco_count = 0
	cols = 'D1' # D is the default range in NAFProduction Google Sheet

	if sheet_name == 'NAFProduction':
		# loop through all the NAFProduction files on the network share
		for n in glob.glob(text_file_location+'NAFProduction_*.txt'):
			with open(n,'r') as statsin:
				statsin = csv.reader(statsin, delimiter='\t',quotechar='', quoting=csv.QUOTE_NONE)
				for row in statsin:
					month_tab = row[1][:-2]
					row = [s.decode('latin1').encode('utf8').strip('"').replace('ß','ǂ') for s in row] # remove quotes

					# write all values to a temp file to check for those that are done within the OnlineSave file
					with open(temp_nafprod_file,'ab') as tempfile:
						writer = csv.writer(tempfile)
						writer.writerow(row)
						
					if row in existing_lines:
						dupe_count += 1
						pass # because it's already in the google sheet
					else:
						post_naco(sheet_name,month_tab,row,cols)
						post_naco_count += 1

	elif sheet_name == 'OnlineSave':
		# loop through all OnlineSave files on network share

		# read in values from the temporary nafprod file
		# TODO? sqlite db instead?
		naf_prod = []
		with open(temp_nafprod_file,'rb') as temp:
			temp_reader = csv.reader(temp, delimiter=',', quotechar='"')
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

		client = gspread.authorize(creds)
		sheet = client.open("OnlineSave").worksheet(this_year)
		update_onlinesave(sheet, naf_prod) # mark any existing rows as DONE

		# loop through the OnlineSave files in the shared directory
		for onlinesave in glob.glob(text_file_location+'OnlineSave_*.txt'):
			if os.stat(onlinesave).st_size > 0:
				with open(onlinesave,'r') as osave:
					osave = csv.reader(osave, delimiter='\t',quotechar='"', quoting=csv.QUOTE_NONE)
					for row in osave:
						cols = 'E1'
						row = [s.decode('latin1').encode('utf8').strip('"').replace('ß','ǂ') for s in row] # remove quotes (from macro)
						user = row[1]
						rtype = row[2]
						try:
							f1xx = row[4]
						except:
							f1xx = 'NULL'  # <= macro error
						#if user not in ['kc','ay','jyn','rs','pt8_17','mis','se','ded']: # skip melt team
						values_to_test = [user,rtype,f1xx] # see if these values can be marked DONE
						online_save.append(values_to_test) # add them to a list
						if (next_row == 2) or (row not in existing_lines): # if the sheet is blank, or if the heading is new, just append
							if values_to_test in naf_prod: # if it's already in one of the NAFProduction files, flag it ...
								row.append('ROBOT')
								row.append('DONE')
								cols = 'G1' # expand the column range to add the above values
							post_naco(sheet_name,this_month,row,cols)
							post_naco_count += 1
							#else:
							#	pass

	logging.info('= %s dupes found in %s' % (dupe_count,sheet_name))
	logging.info('= %s new rows added to %s' % (post_naco_count,sheet_name))
	copyfile(log+log_filename,text_file_location+'logs/'+log_filename)
	logging.info('= %s copied to lib-tsserver' % log_filename)


def update_onlinesave(sheet, naf_prod):
	'''
	Check the OnlineSave Google Sheet against NAFProduction values and flag those that are done
	'''
	next_row = next_available_row(sheet) # next available row
	row_num = sheet.row_count # all the rows in the sheet (even if empty)
	row_num = int(row_num)
	updated = 0
	n = 2 # initial count (skipping the header row)
	if next_row > 2:
		while n <= next_row:
			row_values = sheet.row_values(n)
			time.sleep(1) # seems necessary to avoid api limits
			if row_values:
				#print('checking %s' % row_values)
				if row_values:
					to_test = [row_values[1],row_values[2],row_values[4]]
					to_test = [l.encode('utf8') for l in to_test]
					rowlen = len(row_values) # if there's a note, the row length will be 7 (i.e. skip the ones already marked DONE or ok)
					if to_test in naf_prod and rowlen <= 6:
						cell2update = 'G%s'%n
						sheet.update_acell(cell2update,'DONE')
						updated += 1
			n += 1
	logging.info('= %s headings marked as DONE' % updated)


def post_naco(spreadsheet,month_tab,row,cols):
	'''
	Shows basic usage of the Sheets API.
	Prints values from a sample spreadsheet.
	'''
	# The file token.json stores the user's access and refresh tokens, and is
	# created automatically when the authorization flow completes for the first time.
	range_ = ''
	ws = ''
	insert_data_option = 'OVERWRITE' #'INSERT_ROWS' # append if not already in the sheet (default)
	if spreadsheet == 'OnlineSave':
		ws = this_year
		range_ = 'A1:%s' % cols
	elif spreadsheet == 'NAFProduction': 
		ws = month_tab # <= monthly tabs
		range_ = 'A1:%s' % cols

	value_input_option = 'RAW'

	value_range_body = {
		"values": [
					row
				]
			}

	# Call the Sheets API
	client = gspread.authorize(creds)
	sheet = client.open(spreadsheet).worksheet(ws)
	cell_list = sheet.range(range_)
	sheet.update_cells(cell_list)

	msg = 'posting to %s : %s,%s,%s,%s,%s' % (spreadsheet,row[0],row[1],row[2],row[3],row[4]) # just for feedback
	#print(msg)
	logging.info(msg)


def read_gsheet(sheet_name,scopes,creds):
	'''
	Get all values from Google Sheets
	'''
	# use creds to create a client to interact with the Google Drive API
	sheet = ''
	sheet_values = []
	client = gspread.authorize(creds)
	
	# Find a workbook by name and open the first sheet
	# Make sure you use the right name here.
	if sheet_name == 'NAFProduction':
		wb = client.open("NAFProduction")
		for tab in wb.worksheets():
			sheet = client.open("NAFProduction").worksheet(tab.title)
			#this_sheet_values = sheet.get_all_values() # watch api limits (500 requests per 100 seconds per project, and 100 requests per 100 seconds per user)
			this_sheet_values = get_sheet_values(sheet) # get values from each sheet
			if this_sheet_values:
				for val in this_sheet_values:
					sheet_values.append(val)
	elif sheet_name == 'OnlineSave': 
		sheet = client.open("OnlineSave").sheet1
		sheet_values = get_sheet_values(sheet)

	logging.info('= reading Google Sheet %s' % sheet_name)
	return sheet_values


def get_sheet_values(sheet):
	'''
	attempting to work around limits of get_all_values
	'''
	logging.info('= getting values of %s' % sheet.title)
	sheet_values = []
	next_row = next_available_row(sheet) # next available (blank) row
	logging.info('= Google Sheet %s has %d rows' % (sheet.title,next_row-1))
	n = 2 # initial count (skipping the header row)
	if next_row > 2:
		while n <= next_row:
			row_values = sheet.row_values(n)
			print(row_values)
			if sheet.title == this_year: # OnlineSave sheet will have the name of the current year
				sheet_values.append(row_values[:5])
			else:
				sheet_values.append(row_values)
			time.sleep(1) # painfully slow but otherwise api limit is hit
			n += 1
	# return a list of lists
	return sheet_values


def next_available_row(sheet):
	'''
	Stolen from stackoverflow, to find next blank row
	'''
	str_list = filter(None, sheet.col_values(1))
	return len(str_list)+1


def setup():
	'''
	Create temp file for all nafproduction values
	'''
	if os.path.isfile(temp_nafprod_file):
		cleanup() # remove existing file
	else:
		os.mknod(temp_nafprod_file)
	logging.info('= created %s' % temp_nafprod_file)


def cleanup():
	'''
	Remove temp nafproduction file
	'''
	os.remove(temp_nafprod_file)
	logging.info('= %s removed' % temp_nafprod_file)


if __name__ == "__main__":
	main()
