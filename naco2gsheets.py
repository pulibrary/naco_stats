#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Push locally collated NACO statistics to Google Drive.
If running manually, adjust the cfg file and run `python naco2gsheets.py`
Requires credentials: https://console.developers.google.com/apis
To see quotas see https://console.developers.google.com/apis
from 20181213
pmg
"""
import argparse
import ConfigParser
import csv
import glob
import gspread
import httplib2
import json
import os
import pandas as pd
import logging
import requests
import sqlite3 as lite
import time
from df2gspread import df2gspread as d2g
from googleapiclient.discovery import build
from gsheets import Sheets
from oauth2client import file, client, tools
from oauth2client.service_account import ServiceAccountCredentials
from operator import itemgetter
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

downloaded_os = "OnlineSave%s" % this_year # downloaded Google sheet with latest annotations
nafcsv = 'nafprod_%s.csv' % this_month
os_to_upload = downloaded_os+'_to_upload.csv'
os_emergency_backup = '/onlinesave_backup/OnlineSave_emergency_backup.csv'

scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(conf_dir+'client_secret.json', scopes)
client = gspread.authorize(creds)

log_filename = today+'.log' # <= write out values from all naf prod files temporarily
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=log+log_filename,level=logging.INFO)


def main():
	logging.info('=' * 50)
	logging.info('main()')
	make_temp_nafprod_file() 
	make_temp_onlinesave() # just as a backup on network share
	download_onlinesave()
	gsheets = ['NAFProduction','OnlineSave'] # names of the Google Sheets
	for wb in gsheets:
		make_files_to_upload(wb)
	cleanup()
	logging.info('=' * 50)


def download_onlinesave():
	'''
	Download onlinesave for local parsing.
	'''
	sheets = Sheets.from_files(conf_dir+'/client_secret.json',conf_dir+'./storage.json')
	fileId = online_save_id
	url = 'https://docs.google.com/spreadsheets/d/' + fileId
	s = sheets.get(url)
	sheet_index = 0
	oscsv = 'OnlineSave%s.csv' % this_year

	s.sheets[sheet_index].to_csv(oscsv,encoding='utf-8',dialect='excel')

	msg = '= OnlineSave Google Sheet saved to csv'
	if verbose:
		print(msg)
	logging.info(msg)


def make_temp_nafprod_file():
	'''
	Combine all NAFProduction_*.txt files locally -- these are the most up to date values
	'''
	setup()
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
	
	msg = '= made %s' % temp_nafprod_file
	if verbose:
		print(msg)
	logging.info(msg)


def make_temp_onlinesave():
	'''
	Combine all OnlineSave_*.txt files as a backup on the network share
	'''
	bakfile = text_file_location+os_emergency_backup
	if os.path.isfile(bakfile):
		os.remove(bakfile)
		logging.info('= %s removed' % bakfile)

	for onlinesave in glob.glob(text_file_location+'OnlineSave_*.txt'):
		if os.stat(onlinesave).st_size > 0:
			with open(onlinesave,'r') as osave, open(bakfile, "ab") as outfile:
				osave = csv.reader(osave, delimiter='\t',quotechar='"', quoting=csv.QUOTE_NONE)
				osbak = csv.writer(outfile)
				for row in osave:
					row = [s.decode('latin1').encode('utf8').strip('"').replace('ß','ǂ') for s in row] # remove quotes
					osbak.writerow(row)

	msg = '= made OnlineSave_emergency_backup.csv on networkshare'
	if verbose:
		print(msg)
	logging.info(msg)


def make_files_to_upload(sheet_name):
	'''
	Get values from txt files and put them into lists
	'''
	f1xx = ''
	next_row = 0
	existing_lines = []
	existing_lines_all = []
	new_lines = []
	online_save = []

	logging.info('=' * 25)
	logging.info('= getting data from %s files' % sheet_name)

	post_naco_count = 0

	if sheet_name == 'NAFProduction':
		# loop over the temp NAFProduction file (which combines all the txt files on network share)
		with open(temp_nafprod_file,'rb') as temp, open(nafcsv,'wb+') as nafup:
			temp_reader = csv.reader(temp, delimiter=',', quotechar='"')
			nafup_writer = csv.writer(nafup, delimiter=',', quotechar='"')
			header = ['vgerid','date','type','category','field1xx']
			nafup_writer.writerow(header)
			for row in temp_reader:
				month_tab = row[1][:-2]
				record_date = row[1][:-2]
				if record_date == this_month:
					nafup_writer.writerow(row)
					post_naco_count += 1

		upload_to_gsheets(nafcsv,sheet_name,this_month) # TODO fill in wb and sheet names

	elif sheet_name == 'OnlineSave':
		# TODO: refactor see update_onlinesave
		naf_prod = []
		with open(temp_nafprod_file,'rb') as temp:
			temp_reader = csv.reader(temp, delimiter=',', quotechar='"')
			for row in temp_reader:
				vgerid = row[0]
				rtype = row[2]
				category = row[3]
				try:
					f1xx = row[4]
				except:
					f1xx = 'NULL' # <= this would indicate a macro error
				relevant_values = [vgerid,category,f1xx]
				naf_prod.append(relevant_values) # a list of everything in NAFProduction files to compare against OnlineSave files, to detect and automatically mark new ones

		update_onlinesave() # this checks the downloaded OnlineSave Google Sheet and marks DONE, produces onlinesave{year}_out.csv
		
		with open(downloaded_os+'_out.csv','rb') as osout:
			osout_reader = csv.reader(osout, delimiter=',', quotechar='"')
			next(osout_reader,None)
			for line in osout_reader:
				existing_lines_all.append(line) # full line including annotations
				if len(line) == 8:
					line = line[:-3] # remove the last cell (notes)
				elif len(line) == 7:
					line = line[:-2] # remove the last cell (DONE,ok)
				elif len(line) == 6:
					line = line[:-1] # remove last cell (assignment)
				existing_lines.append(line) # no annotations

		# now loop through the OnlineSave files in the shared directory for the newer records ...
		for onlinesave in glob.glob(text_file_location+'OnlineSave_*.txt'):
			if os.stat(onlinesave).st_size > 0:
				with open(onlinesave,'r') as osave:
					osave = csv.reader(osave, delimiter='\t',quotechar='"', quoting=csv.QUOTE_NONE)
					for row in osave:
						row = [s.decode('latin1').encode('utf8').strip('"').replace('ß','ǂ') for s in row] # remove quotes (from macro)
						user = row[1]
						rtype = row[2]
						try:
							f1xx = row[4]
						except:
							f1xx = 'NULL'  # <= this would be a macro error
						#if user not in ['kc','ay','jyn','rs','pt8_17','mis','se','ded']: # skip melt team
						values_to_test = [user,rtype,f1xx] # see if these values can be marked DONE
						online_save.append(values_to_test) # add them to a list
						if row not in existing_lines:
							if values_to_test in naf_prod: # if it's already in one of the NAFProduction files, flag it ...
								row.append('ROBOT')
								row.append('DONE')
							new_lines.append(row)
						post_naco_count += 1
		with open(os_to_upload,'wb+') as osup:
			osup_writer = csv.writer(osup)
			header = ['fileid','vgerid','type','date','1xx','reviewer','is_done','notes'] # if fields are added to gsheet, add them here
			osup_writer.writerow(header)
			allout = existing_lines_all + new_lines
			allout = sorted(allout,key=itemgetter(3)) # sort by date
			for new in allout:
				osup_writer.writerow(new)
		logging.info('= wrote to  %s' % os_to_upload)

		upload_to_gsheets(os_to_upload,sheet_name,this_year)

	msg = '= %s rows uploaded to %s' % (post_naco_count,sheet_name)
	if verbose:
		print msg
	logging.info(msg)
	

def update_onlinesave():
	'''
	Check the (downloaded) OnlineSave Google Sheet against NAFProduction text files and flag those that are already done
	'''
	naf_prod = []
	to_test = []

	with open('naf_prod_temp.csv','r') as naftemp:
		naftempreader = csv.reader(naftemp)
		for l in naftempreader:
			l = [s.strip('"').replace('ß','ǂ') for s in l]
			to_compare = l[0],l[3],l[4]
			to_compare = list(to_compare)
			naf_prod.append(to_compare)
			
	updated = 0
	with open(downloaded_os+'.csv','r') as os,open(downloaded_os+'_out.csv','w') as osout:
		osreader = csv.reader(os, delimiter=',', quotechar='"')
		oswriter = csv.writer(osout)
		for row in osreader:
			if row:
				row = row[1:] # to remove pandas index
				to_test = [row[1],row[2],row[4]] # vgerid, type, 1xx
				rowlen = len(row) # if there's a note, the row length will be 7 (i.e. skip the ones already marked DONE or ok)
				if to_test in naf_prod and rowlen <= 6:
					row.append('ROBOT')
					row.append('DONE') # TODO: change this :)
					updated += 1
				oswriter.writerow(row)

	msg = '= %s headings marked as DONE' % updated
	if verbose:
		print(msg)
	logging.info(msg)


def upload_to_gsheets(file_to_upload,workbook,sheetname):
	'''
	Upload parsed csv files
	'''
	# when token expires, try going to google api, oauth 2.0 OAuth client IDs and grab another or create another and download, save it as ``~/.gdrive_private`` (as a file)
	
	sheet = client.open(workbook).id

	df = pd.read_csv(file_to_upload)
	df.fillna('', inplace=True)

	d2g.upload(df,sheet,sheetname)

	msg = '= uploaded %s to %s' % (file_to_upload, workbook)# TODO variables
	if verbose:
		print(msg)
	logging.info(msg)


def setup():
	'''
	Create temp file for all nafproduction values
	'''
	if os.path.isfile(temp_nafprod_file):
		cleanup() # remove existing file
	else:
		os.mknod(temp_nafprod_file)
	msg = '= created %s' % temp_nafprod_file
	if verbose:
		print(msg)
	logging.info(msg)


def cleanup():
	'''
	Remove temp nafproduction file
	'''
	copyfile(log+log_filename,text_file_location+'logs/'+log_filename)
	msg = '= %s copied to lib-tsserver' % log_filename
	if verbose:
		print(msg)
	logging.info(msg)
	
	os.remove(temp_nafprod_file)
	msg = '= %s removed' % temp_nafprod_file
	if verbose:
		print(msg)
	logging.info(msg)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Parse NACO stats and send updates to Google Drive.')
	parser.add_argument("-v", "--verbose", required=False, default=False, dest="verbose", action="store_true", help="Runtime feedback.")
	args = vars(parser.parse_args())
	verbose = args['verbose']
	main()
