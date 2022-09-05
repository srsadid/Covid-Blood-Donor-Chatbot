
import pandas as pd
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

cred = {
  "type": "service_account",
  "project_id": "covid-chatbot-dllawx",
  "private_key_id": "c8204137ca3c89681f92446cfd6b9adbbb910eb5",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDAe2UqF7icO22G\nafKq+6wnvWW9RrH6n0Evlgy4CAHDEopZ4cqVKH2GhrIN9VVAS7J99cysQptyb1hr\nJYxz+1kqH/ksvaSH4CCG/J474/yEBonopV/LQ9LduzfweX2KvOszXHXAlPJwJTAx\nHrhzbQBiNfFo2hJjiaQMv8IaXwKWoMPo0gUxhtJEwBK6c0h1DBQxLshGDap7IQxB\nbzSJHtqlpDi+cEL1zENVrkMIqQvTbtxZsYyMcch4iDRy5zLUC+2+0RAZt6ebgjMk\nBMNtZidpEqCijVUQezGrvbVFE9rlKIN1xFfWCUepcRjpi1yaW+pklwqMxLvfvL9N\nRKGFUbAlAgMBAAECggEABn4C/wT2zpo4iwCW1UqQ13RNcrFoPmZRugurGSf3+z7s\nWLTDcv1/ImL3rX3ZZsZG2nXIdgT3HkiAKHkHQEA5lTCn5GjvHEAkiLkLk0SQ11kr\nHKQ7U21RtvXiIKPQZrYRjtVvCVRcywiOFBIPsaD7EtTz9A/q792MIXv0TZRnz3HE\nnuGrON9cUpfGaW221c4ARK4Bk+xQw7c+gOBIYTqX8k/qQJTLAirmuwX3ui5dTsXp\nOMIPZhent91avNEjQGuoOR4TGEiUrEWAsH0dkoysUyAxY6tXvOjWghtBnfnCZlHc\n7ZGOHoh8vg3B+/PZBJM9NBQ9fqMWL/De+Xq99zkBrwKBgQD9UlVUwF7BoXgZ60a3\nqv2rI+f6M9UUbYzfZFqubgvoKCT9gPz9e61Msvdm6WvtGFRNKbk4EvOdl4cW9xGk\n0fq44JH2sPRBI/7YZF3oiOz751mHZDDHYvstB4OlJAn4xYwo2STcQA9dLvb2WoGD\nx/XmCtM4E1Oc1+yNQUG51VWxRwKBgQDChGMVQCit6haT+OjJIQybWcu53tA4As6x\nOLYbu2rTz65NE0o0vzZtSugbvi237sm5Uq5AMKQ7uiQxrEHyHq7dzfDJV6kslBka\nPjKhuH6grDk92xWwGi4IleS+DNgKiEX8/JMC5pF7AXRl4ODac8BRNlVUfIBe5aRt\nw8c1+s0pMwKBgAmtw6ThVL2BNd2Hp8QMvHR1gr+Ei3ekV+WRKAXSHpJYNlGZRBFL\nUGvFyr4b3QvKCi6IkZMa5kP3Lioqdnodq760ld8fE7YJcgtCinQAB034oOsTYOm9\nVmt6BgWhQuBGa/yDj2z65Zth1/3Bp4EperO3ZiqWUnODrH4ZKDG5fPSxAoGABcjk\nHXSsAQgOJdZoLdsSwef4vA1ZSeEjfne+SrLXKXdqqpYhRdJN7xAkOPb96xNp4l4I\nGpaXu8L8YpHJ4EZzUttWBtbZjB7XZEVnQhH3ihW1GhbS3UraBZ1XkSNWpXGKC00D\nnjl3KB5R9NmsNjLtDD0+amXNd4UCz9TbrspHoasCgYEAqtymItYBPT7cr2TTfe44\nTuy+tMPvtWK8LHYiye2ojPLRVPBISHCWteCqWcipfBd0ylVgj6QBjb8VL6EzYdzL\ntrEYCroN9A4CTC6s/4keIUip/nwmqb1IVuyVmJVG2BWUSOzaTesbHSJucwjcwt5Q\nWBl6tdCTqW3k2IJ156Z5aJc=\n-----END PRIVATE KEY-----\n",
  "client_email": "936030275429-compute@developer.gserviceaccount.com",
  "client_id": "101851626372795103270",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/936030275429-compute%40developer.gserviceaccount.com"
}

credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred, scope)
#service = build('sheets', 'v4', credentials=credentials)

gc = gspread.authorize(credentials)


spreadsheet_key = '1aDRkfQoHDBUGl4LogY5MbfuqTFy_x1bsNJcpOYOXgyc'

wks_name = 'New Master'

read_sheet = gc.open_by_key(spreadsheet_key).worksheet(wks_name)

data = read_sheet.get_all_values()
headers = data.pop(0)

df = pd.DataFrame(data, columns=headers)

#df = df.drop_duplicates() #run only for first time 


spreadsheet_key = '1aDRkfQoHDBUGl4LogY5MbfuqTFy_x1bsNJcpOYOXgyc'
wks_name =  'New Sheet'
read_sheet = gc.open_by_key(spreadsheet_key).worksheet(wks_name)
data = read_sheet.get_all_values()
headers = data.pop(0)
df_temp = pd.DataFrame(data, columns=headers)
#df_temp =pd.read_csv('File Name.csv')
df_temp  =  df_temp.iloc[:,1:] 


df_join = pd.concat([df, df_temp])
#df_res----------------------^^^
df_join = df_join.drop_duplicates(keep = False)
df_join


b_group = ["A+","a+","B+","b+","Ab+","ab+","O+","o+","A-","a-","B-","b-","AB-","ab-","O-","o-"]

spreadsheet_key = "1TxtXGOR6DXrGfUpwJiyGS9Yv9g4irNfxNk0m3ChnM5Y"

for index, item in enumerate(b_group):
    next1 = index + 1
    if next1 < len(b_group):
        if index == 0 or index == 2 or index ==4 or index == 6 or index == 8 or index == 10 or index == 12 or index == 14:
            #print (index)
            #print (item , b_group[next1])
            #print(item)
            wks_name = item
            upper =  df_join.loc[df_join['Blood Group'] == item]
            lower  = df_join.loc[df_join['Blood Group'] == b_group[next1]]
            sorted_b_group = pd.concat([upper, lower])
            if len(sorted_b_group) > 0 :
                print(sorted_b_group)
                print("worksheet name:::::" ,wks_name)
                #for the first time (replases all the value)
                #d2g.upload(sorted_b_group, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
                #print("uploaded to >>>" , wks_name )
                #r'''
                #for appending to the existing value 
                ws = gc.open_by_key(spreadsheet_key).worksheet(wks_name)
                data = ws.get_all_values()
                headers = data.pop(0)

                df1 = pd.DataFrame(data, columns=headers)
                df1.reset_index(drop=True) 
                updated = df1.append(sorted_b_group)
                gd.set_with_dataframe(ws, updated)
                
                print("uploaded to >>>" , wks_name )
                #r'''



spreadsheet_key = '1aDRkfQoHDBUGl4LogY5MbfuqTFy_x1bsNJcpOYOXgyc'
wks_name = "New Sheet"
df_temp = df
d2g.upload(df_temp, spreadsheet_key, wks_name, credentials=credentials, row_names=True)
#df_temp.to_csv('File Name.csv' , index = False)
print("temp  uploaded")