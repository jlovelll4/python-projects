import os
import arcpy
import scandir
from datetime import datetime
import sys
import traceback
import csv
import smtplib


try:

    def link_progressbar(remain_count, total_count):
        ''' Shows progression of number of links checked. It also shows a dynamic count of good and bad links.'''
        bar_size = 100
        percent = float(remain_count) / total_count
        hashes = '#' * int(round(percent * bar_size))
        spaces = '-' * (bar_size - len(hashes))
        sys.stdout.write("\rProcessed Links: {6} / {7}  | PMT Good Count: {4} | PMT Bad Count: {5} | PR "
                         "Good Count: {3}  |  PR "
                         "Bad Count: {2}  |  Percent: [{0}] {"
                         "1:.2f}%".format
                         (hashes + spaces, float(percent * 100), PR_bad_count, PR_good_count, PMT_good_count,
                          PMT_bad_count, ftrcntprog, total_count))
        sys.stdout.flush()


    def write_to_csv(csv_file, object_variable, data_to_write):
        ''' Used to quickly write to a csv file....helps declutter the body of the script.'''
        with open(csv_file, "a+b") as object_variable:
            csv_writer = csv.writer(object_variable, delimiter=',')
            csv_writer.writerow(data_to_write)


    # Starts the timer
    start_time = datetime.now()

    # file objects
    simple_log_file = # Output log file location. Logs script runtime. 
    uncorrected_links_csv = # Path to csv file which logs links not updated in Geodatabase.
    duplicate_directories_csv = # Path to csv file which logs redundant PR / PMT links found in Geodatabase.
    changes_csv = # Path to csv file which logs changes made to Geodatabase.

    # Root path used to search directories top down.
    root_path = # Path to root directory.
    # ROW Research SDE
    workspace = # Path to SDE
    # Set environment for arcpy.
    arcpy.env.workspace = workspace
    # Feature Classes located in the SDE which are checked for bad links.
    parcel_FC = # parcel FC in SDE
    landties_FC = # landties FC in SDE

    # Variables used to
    PR_bad_count = 0
    PR_good_count = 0
    PMT_good_count = 0
    PMT_bad_count = 0
    ftrcntprog = 0
    duplicate_counter = 0
    uncorrected_counter = 0
    directory_dict = {}
    duplicate_dict = {}
    uncorrected_links = []
    duplicate_directories = []
    good_link_dict = {}

    # Copies FC data to memory
    parcel_table = arcpy.MakeTableView_management(parcel_FC, 'parcel_table')
    landties_table = arcpy.MakeTableView_management(landties_FC, 'landties_table')

    landties_count = 0
    parcel_count = 0
    with arcpy.da.SearchCursor(landties_table, ['PR_LINK']) as cursor:
        for row in cursor:
            landties_count += 1
    with arcpy.da.SearchCursor(landties_table, ['PMT_LINK']) as cursor:
        for row in cursor:
            landties_count += 1
    with arcpy.da.SearchCursor(parcel_table, ['PR_LINK']) as cursor:
        for row in cursor:
            parcel_count += 1
    with arcpy.da.SearchCursor(parcel_table, ['PMT_LINK']) as cursor:
        for row in cursor:
            parcel_count += 1

    total_count = int(parcel_count + landties_count)
    print("Total Directories: 62555")
    print("LINK COUNT: {}".format(total_count))
    max = 5000
    directory_count = 0
    for root, dirnames, filenames in scandir.walk(root_path, topdown=True):
        for dirname in dirnames:
            if dirname not in directory_dict:
                directory_dict[dirname] = []
            directory_dict[dirname].append(os.path.join(root, dirname))
            directory_count += 1
            if directory_count % 100 == 0:
                print("Processed {} / 62555".format(directory_count))
            if directory_count > max:
                break
        if directory_count > max:
            break
    print("DIRECTORY COUNT: {}".format(directory_count))

    # Creates csv files and writes the column names
    uncorrected_columns = ['FEATURE CLASS', 'OID', 'PARCEL NUMBER', 'PR_LINK', 'PMT LINK']
    write_to_csv(uncorrected_links_csv, 'csv_file', uncorrected_columns)

    duplicate_columns = ['PARCEL NUMBER', 'LINK']
    write_to_csv(duplicate_directories_csv, 'csv_file', duplicate_columns)

    changes_columns = ['FEATURE CLASS', 'OID', 'PARCEL NUMBER', 'OLD PR LINK', 'NEW PR LINK', 'OLD PMT LINK',
                       'NEW PMT LINK']
    write_to_csv(changes_csv, 'csv_file', changes_columns)

    # Checks feature class tables for bad links. If bad, it check the directory dictionary for a good link. If that
    # link exists (and only one is present) the bad link will be replaced.
    fields = ['OBJECTID', 'PARCEL_NUM', 'PR_LINK', 'PMT_LINK']
    feature_classes = [landties_FC, parcel_FC]
    for fc in feature_classes:
        with arcpy.da.UpdateCursor(fc, fields) as cursor:
            for row in cursor:
                object_id = row[0]
                parcel_num = row[1]
                old_pr_link = row[2]
                old_pmt_link = row[3]

                # Check PMT LINK
                if os.path.exists(old_pmt_link):
                    ftrcntprog += 1
                    PMT_good_count += 1
                    if os.path.exists(old_pr_link):
                        ftrcntprog += 1
                        PR_good_count += 1
                    else:
                        
                else:
                    PMT_bad_count += 1
                    ftrcntprog += 1
                    if directory_dict.has_key(parcel_num):

                        if len(directory_dict[parcel_num]) > 1:
                            uncorrected_counter += 1
                            line = [fc, object_id, parcel_num, '', old_pmt_link]
                            write_to_csv(uncorrected_links_csv, 'broke', line)
                            for link in directory_dict[parcel_num]:
                                if link in duplicate_dict:
                                    pass
                                else:
                                    line = [parcel_num, link]
                                    duplicate_dict[parcel_num] = link
                                    write_to_csv(duplicate_directories_csv, 'dup', line)
                        else:
                            new_pmt_link = directory_dict[parcel_num][0]
                            idx = new_pmt_link.rfind('\\')
                            parcel_parent_path = new_pmt_link[:idx]
                            idx = parcel_parent_path.rfind('\\')
                            parcel_root_path = parcel_parent_path[:idx]
                            newpr_path = (parcel_root_path + "\PURCHASE REPORT")
                            row[3] = new_pmt_link
                            row[2] = newpr_path
                            cursor.updateRow(row)
                            change_line = [fc, object_id, parcel_num, old_pr_link, newpr_path, old_pmt_link, new_pmt_link]
                            write_to_csv(changes_csv, "change", change_line)

                    else:
                        uncorrected_counter += 1
                        line = [fc, object_id, parcel_num, '', old_pmt_link]
                        write_to_csv(uncorrected_links_csv, 'broke', line)

                # Ensures the PR_Link is good.
                if os.path.exists(old_pr_link):
                    ftrcntprog += 1
                    PR_good_count += 1
                link_progressbar(ftrcntprog, total_count)

    date = datetime.now() - start_time
    date_reformat = date.strftime("%m/%d/%Y %H:%M:%S")
    auto_email = # From email.
    FROM = auto_email
    TO = [#email]
    SUBJECT = # Email's Subject
    TEXT = "START TIME: {}\n\nEND TIME: {}\n\nSCRIPT RUN TIME: {}\n\n".format(
        start_time, datetime.now(), date_reformat)
    message = "From: {}\r\nTo: {}\r\nSubject: {}\r\n{}".format(FROM, ", ".join(TO), SUBJECT, TEXT)

    # Send the email
    server = smtplib.SMTP(# Exchange server, 25)
    server.sendmail(FROM, TO, message)
    server.quit()


except Exception:
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    pymsg = "PYTHON ERRORS:\nTraceback info:\n {}\nError Info:\n {}:{}\n".format(tbinfo,
                                                                                 str(sys.exc_type),
                                                                                 str(sys.exc_value))
    arcpy.AddError(pymsg)
    arcmsg = "ARCPY ERRORS:\n{}\n".format(arcpy.GetMessages(2))
    arcpy.AddError(arcmsg)
    print("{}\n".format(pymsg))
    print("{}\n".format(arcmsg))

    # Setup error message.
    date = datetime.now()
    error_date = date.strftime("%m/%d/%Y %H:%M:%S")
    auto_email = # From email
    FROM = auto_email
    TO = [# Email]
    SUBJECT = # Email's Subject
    TEXT = "ERROR | ERROR DATE: {}\n\nPYTHON ERROR INFO: {}\n\nARCPY ERROR INFO: {" \
           "}\n\nSTART TIME: {}\n\nEND TIME: {}\n\nSCRIPT RUN TIME: {}\n\n".format(
        error_date, pymsg, arcmsg, start_time, datetime.now(), datetime.now() - start_time)
    message = "From: {}\r\nTo: {}\r\nSubject: {}\r\n{}".format(FROM, ", ".join(TO), SUBJECT, TEXT)

    # Send the email
    server = smtplib.SMTP(# Exchange Server, 25)
    server.sendmail(FROM, TO, message)
    server.quit()


else:

    line = "FINDANDFIXBADLINKS.PY | START TIME: {} | END TIME: {}  SCRIPT RUN TIME: {}\n".format(start_time,
                                                                                                 datetime.now(),
                                                                                                 datetime.now() -
                                                                                                 start_time)
    with open(simple_log_file, "a+") as log_file:
        log_file.write(line)
        print(line)
