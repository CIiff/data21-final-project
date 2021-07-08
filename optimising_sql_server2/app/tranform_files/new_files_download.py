from pathlib import Path
import pandas as pd
import csv
import os


def get_new_files_list(Downloadlist):

    fileList = [file.name for file in Path(os.getcwd()).iterdir()]

    if 'file_memory.csv' in fileList:
        with open('file_memory.csv', mode='r') as csv_file:
            current_file_list = (pd.read_csv(
                csv_file, header=0).values.tolist())
            flatList = [
                file_name for file in current_file_list for file_name in file]
            # compare flatList with extraction list
            # get the difference
            new_files = list(set(Downloadlist).difference(set(flatList)))
            csv_file.close()
            if new_files != []:

                with open('file_memory.csv', 'a') as csv_file:
                    write = csv.writer(csv_file)
                    write.writerows(map(lambda x: [x], new_files))
            else:
                print('No new files')

        return new_files
    else:
        with open('file_memory.csv', mode='w') as csv_file:
            write = csv.writer(csv_file)
            write.writerow(['File_names'])
            write.writerows(map(lambda x: [x], Downloadlist))
        return Downloadlist
