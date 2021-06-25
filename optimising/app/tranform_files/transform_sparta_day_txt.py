from optimising.app.load_files.get_files_from_s3 import getFiles,logger,pd,tqdm




class transformTxtFiles():

    sparta_day_df = pd.DataFrame()



    def __init__(self):
        self.txt_files = getFiles('data21-final-project','Talent','.txt')
        self.txt_files_list = self.txt_files.get_list_of_files()
        self.txt_file_dict = self.txt_files.create_dict_of_txt_files()
        
        pass


    def make_dataframe(self):

      
        for sparta_day in tqdm(self.txt_file_dict,unit ='txt_files',desc = 'Transforming_txt_files',position = 0):
            # logger.debug(f'Tranforming {sparta_day} .txt file')
            
            contents = self.txt_file_dict[sparta_day]
            
            text_lines_list = contents.decode("utf-8").split('\r\n')
                
            text_lines_list2 = [line.replace("-  Psychometrics",",Pychometrics") for line in text_lines_list]
            text_lines_list3 = [line.replace(" - ","-") for line in text_lines_list2]
            # pprint(text_lines_list3)

       
           
            flattened_list = [item.split(",") for item in text_lines_list3]
            date,location = flattened_list[0][0],flattened_list[1][0]
            # print(location)
            # print(date)
            for spartan in flattened_list[3:-1]:


                spartan_phsycometrics = [scores.split(":")[1].strip().split("/") for scores in spartan[1].split(",")]
                spartan_presentation = [scores.split(":")[1].strip().split("/") for scores in spartan[2].split(",")]
                spartan_row =[spartan[0].strip().title()]+spartan_phsycometrics + spartan_presentation
                # pprint(spartan_row)
                candidate_name,presentation,presentation_max,psychometrics,psychometrics_max = spartan_row[0],spartan_row[1][0],spartan_row[1][1],spartan_row[2][0],spartan_row[2][1]
                # print( candidate_name,presentation,presentation_max,psychometrics,psychometrics_max )

                sparta_day_details = {
                                    'candidate_name':candidate_name,
                                    'presentation':presentation,
                                    'presentation_max':presentation_max,
                                    'psychometrics':psychometrics,
                                    'psychometrics_max':psychometrics_max,
                                    'location':location,
                                    'date':date
                                    }

                self.sparta_day_df = self.sparta_day_df.append(sparta_day_details,ignore_index=True)
        self.sparta_day_df['date'] = self.sparta_day_df['date'].astype('datetime64')
        self.sparta_day_df['date'] = self.sparta_day_df['date'].astype(str)

        return self.sparta_day_df.drop_duplicates().replace({pd.NaT: None})




sparta_day = transformTxtFiles()
sparta_day_df = sparta_day.make_dataframe()
# print(sparta_day_df)







    # text_lines_list =['Tuesday 7 May 2019',
    # 'London Academy',
    # '',
    # 'LAURELLA DAINTREY -  Psychometrics: 55/100, Presentation: 25/32',
    # 'OMAR BROMILOW -  Psychometrics: 62/100, Presentation: 22/32',
    # 'FABIO VEDISHCHEV -  Psychometrics: 40/100, Presentation: 13/32',
    # 'FAX SCHANKELBORG -  Psychometrics: 50/100, Presentation: 25/32',
    # 'ALBIE ROMANSKI -  Psychometrics: 55/100, Presentation: 22/32',
    # 'WINNIFRED HINGELEY -  Psychometrics: 56/100, Presentation: 22/32',
    # 'PRISCA GINNALLY -  Psychometrics: 51/100, Presentation: 23/32',
    # 'VALENTINE BROSINI -  Psychometrics: 51/100, Presentation: 17/32',
    # 'FIFINE WREIGHT -  Psychometrics: 65/100, Presentation: 16/32',
    # 'FREE NICHOLES -  Psychometrics: 58/100, Presentation: 24/32',
    # 'MAGGY CLAPISON -  Psychometrics: 56/100, Presentation: 20/32',
    # 'DORY COOKSEY -  Psychometrics: 54/100, Presentation: 21/32',
    # 'FRANCISCO TEBBOTH -  Psychometrics: 62/100, Presentation: 16/32',
    # 'GAEL EICK -  Psychometrics: 50/100, Presentation: 19/32',
    # 'TEODOR PATTLEL -  Psychometrics: 47/100, Presentation: 16/32',
    # 'BONDON SMETHURST -  Psychometrics: 57/100, Presentation: 15/32',
    # 'STACEE POINTS -  Psychometrics: 45/100, Presentation: 17/32',
    # 'GUNTER YOKLEY -  Psychometrics: 58/100, Presentation: 20/32',
    # 'AUBERON HATTER -  Psychometrics: 68/100, Presentation: 24/32',
    # 'ENOCH BRIGG -  Psychometrics: 63/100, Presentation: 20/32',
    # 'TERRANCE WOLFINGER -  Psychometrics: 62/100, Presentation: 23/32',
    # 'SILVIE ALLDERIDGE -  Psychometrics: 51/100, Presentation: 21/32',
    # 'LOREE ABBOT -  Psychometrics: 60/100, Presentation: 17/32',
    # 'BROOKE CRIDLIN -  Psychometrics: 59/100, Presentation: 14/32',
    # 'ROZE KYLES -  Psychometrics: 58/100, Presentation: 20/32',
    # 'ERROL ALLDERIDGE -  Psychometrics: 46/100, Presentation: 16/32',
    # '']








