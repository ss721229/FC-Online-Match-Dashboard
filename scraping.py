import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
import json

class FCOnlineMatch:
    def __init__(self):
        self.s3 = boto3.client('s3', aws_access_key_id='#', aws_secret_access_key='#', region_name='#')
        self.bucket_name = 'fc-online-match'
        self.file_key = ['match_detail.csv', 'shoot.csv', 'pass.csv', 'defence.csv']
        self.headers = {"x-nxopen-api-key": "#"}
        
        self.initialize_csv_file_in_s3()

    def initialize_csv_file_in_s3(self):
        cols = [['matchId', 'seasonId', 'matchResult', 'matchEndType', 'systemPause', 'foul', 'injury',
                'redCards', 'yellowCards', 'dribble', 'cornerKick', 'possession', 'offsideCount', 'averageRating', 'controller'],
                ['matchId', 'shootTotal', 'effectiveShootTotal', 'shootOutScore', 'goalTotal', 'goalTotalDisplay','ownGoal','shootHeading',
                'goalHeading', 'shootFreekick', 'goalFreekick', 'shootInPenalty', 'goalInPenalty', 'shootOutPenalty', 'goalOutPenalty', 'shootPenaltyKick', 'goalPenaltykick'],
                ['matchId', 'passTry', 'passSuccess', 'shortPassTry', 'shortPassSuccess', 'longPassTry', 'longPassSuccess','bouncingLobPassTry',
                'bouncingLobPassSuccess', 'drivenGroundPassTry', 'drivenGroundPassSuccess', 'throughPassTry', 'throughPassSuccess', 'lobbedThroughPassTry', 'lobbedThroughPassSuccess'],
                ['matchId', 'blockTry', 'blockSuccess', 'tackleTry', 'tackleSuccess']]

        for k, col in zip(self.file_key, cols):
            try:
                self.s3.head_object(Bucket=self.bucket_name, Key=k)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    print(f"File {k} does not exist in bucket {self.bucket_name}. Uploading the file.")
                    try:
                        # 파일 업로드
                        csv_buffer = pd.DataFrame(columns=col).to_csv(index=False)
                        self.s3.put_object(Bucket=self.bucket_name, Key=k, Body=csv_buffer)
                        print(f"File {k} has been uploaded to bucket {self.bucket_name}.")
                    except ClientError as upload_error:
                        print(f"An error occurred while uploading the file: {upload_error}")
                else:
                    print(f"An error occurred: {e}")

    def get_match_id(self):
        url = "https://open.api.nexon.com/fconline/v1/match?matchtype=50&offset=0&limit=100&orderby=desc"
        response = requests.get(url, headers = self.headers)
        return response.json()
    
    def get_csv_files_in_s3(self):
        match_detail = pd.read_csv(self.s3.get_object(Bucket=self.bucket_name, Key='match_detail.csv')['Body'])
        shoot = pd.read_csv(self.s3.get_object(Bucket=self.bucket_name, Key='shoot.csv')['Body'])
        pass_ = pd.read_csv(self.s3.get_object(Bucket=self.bucket_name, Key='pass.csv')['Body'])
        defence = pd.read_csv(self.s3.get_object(Bucket=self.bucket_name, Key='defence.csv')['Body'])
        return match_detail, shoot, pass_, defence

    def append_match_data_to_csv(self, match_detail, shoot, pass_, defence):
        match_id = self.get_match_id()
        for id in match_id:
            url = f'https://open.api.nexon.com/fconline/v1/match-detail?matchid={id}'
            response = requests.get(url, headers=self.headers)
            if 'matchInfo' in json.loads(response.text):
                for match_info in json.loads(response.text)['matchInfo']:
                    match_info['matchDetail']['matchId'] = id
                    match_info['shoot']['matchId'] = id
                    match_info['pass']['matchId'] = id
                    match_info['defence']['matchId'] = id

                    match_detail = match_detail._append(match_info['matchDetail'], ignore_index=True)
                    shoot = shoot._append(match_info['shoot'], ignore_index=True)
                    pass_ = pass_._append(match_info['pass'], ignore_index=True)
                    defence = defence._append(match_info['defence'], ignore_index=True)
        match_detail['matchResult'] = match_detail['matchResult'].replace({'패': 'lose', '승': 'win', '무': 'draw'})
        return match_detail, shoot, pass_, defence
    
    def save_csv_to_s3(self, match_detail, shoot, pass_, defence):
        try:
            # 파일 업로드
            csv_buffer = match_detail.to_csv(index=False)
            self.s3.put_object(Bucket=self.bucket_name, Key='match_detail.csv', Body=csv_buffer)
            csv_buffer = shoot.to_csv(index=False)
            self.s3.put_object(Bucket=self.bucket_name, Key='shoot.csv', Body=csv_buffer)
            csv_buffer = pass_.to_csv(index=False)
            self.s3.put_object(Bucket=self.bucket_name, Key='pass.csv', Body=csv_buffer)
            csv_buffer = defence.to_csv(index=False)
            self.s3.put_object(Bucket=self.bucket_name, Key='defence.csv', Body=csv_buffer)
        except ClientError as upload_error:
            print(f"An error occurred while uploading the file: {upload_error}")
        
if __name__=='__main__':
    test = FCOnlineMatch()
    match_detail, shoot, pass_, defence = test.get_csv_files_in_s3()
    match_detail, shoot, pass_, defence = test.append_match_data_to_csv(match_detail, shoot, pass_, defence)
    test.save_csv_to_s3(match_detail, shoot, pass_, defence)