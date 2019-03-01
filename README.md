# naco2gsheets

An experimental script to push locally collated NACO statistics to Google Drive for the PUL NACO committee. We're aiming to move beyond redundant manual work and printouts to do these stats. There are two Google Sheets within a private Team Drive:
* NAFProduction - authorities that are actually producted by the NACO Committee. The sheet has one tab per month for the calendar year.
* OnlineSave - authorities that are saved to the *online save file*. This sheet has a single tab for the calendar year, and is manually annotated by NACO Committee members.

NOTE: as of 201902, Tableau can't read from Team Drives, so for compatability with Tableau we need to use a personal Google account, create empty sheets in our personal Drive, use `IMPORTRANGE()` to copy data from the Team Drive, and then point Tableau to these personal sheets.   

Helpful hints: [https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html]([https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html])
