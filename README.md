# naco2gsheets

A script to push locally collated NACO statistics to Google Drive for the PUL NACO committee. We're aiming to move beyond redundant manual work and printouts to do these stats. Text files are output from OCLC Connexion by OML macros (see `./mbk`). These data are then pushed to two Google Sheets within a private Team Drive ...
* NAFProduction - authorities that are actually producted by the NACO Committee. The sheet has one tab per month for the calendar year.
* OnlineSave - authorities that are saved to the *online save file*. This sheet has a single tab for the calendar year, and is manually annotated by NACO Committee members.

The results are available to the NACO committee via Tableau dashboard. As of early 2019, Tableau can't read from Team Drives. A workaround is to create sheets in a personal Google account, use `=IMPORTRANGE()` to copy data from the sheets in the Team Drive, and then point Tableau to the personal sheets.  

Basic workflow ...
`oclc connexion > oml macros > txt files > network share > py script <=> Google Drive`


## Required
* gspread `pip install gspread`
* Google API client library `pip install --upgrade google-api-python-client`
* oauth2client `pip install oauth2client`

[helpful Google API hints](https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html)
