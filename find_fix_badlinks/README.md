Python 2.x



Once upon a time, there existed an RDBMS holding some bad data that needed to be changed. The data consisted of hundreds of thousands of directory links that needed to be checked against a network directory to ensure the paths were good.

This script checks if the links [in the database] are bad, attempts to find the correct file path - where the link (file path) actually exists - and makes the changes to the RDBMS using ArcSDE. ArcSDE, now known as Enterprise, is software that enables ArcGIS applications to store, manage, and retrieve data in a RDBMS.

The script spits out a few files showing run time and corrected / uncorrected / duplicate links. It also generates an email upon completion or if error is thrown during execution. 

