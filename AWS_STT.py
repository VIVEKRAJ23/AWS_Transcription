import pandas as pd
import time
import boto3
import json
import datetime
from botocore.exceptions import NoCredentialsError


ACCESS_KEY = "YOUR_ACCESS_KEY"
SECRET_KEY = 'YOUR_SECRET_KEY'

file_path = "C:/Users/Downloads/"   # Path to save the output
local_file = "Airflow_call.wav"     # Your audio file name
bucket_name = "YOUR_BUCKET_NAME"        

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


transcribe = boto3.client('transcribe',
                          aws_access_key_id = ACCESS_KEY,
                          aws_secret_access_key =  SECRET_KEY,
                          region_name =  "ap-south-1"
)


def check_job_name(job_name):
  job_verification = True
  
  # all the transcriptions
  existed_jobs = transcribe.list_transcription_jobs()
#   print("Existed jobs:",existed_jobs)
  
  for job in existed_jobs['TranscriptionJobSummaries']:
    if job_name == job['TranscriptionJobName']:
      job_verification = False
      break


  if job_verification == False:

    transcribe.delete_transcription_job(TranscriptionJobName=job_name)

  return job_name


def amazon_transcribe(audio_file_name, max_speakers = -1):

  if max_speakers > 10:
    raise ValueError("Maximum detected speakers is 10.")

  job_uri = "s3://mystt-bucket/" + audio_file_name 
  job_name = (audio_file_name.split('.')[0]).replace(" ", "")
  
  # check if name is taken or not
  job_name = check_job_name(job_name)

  uploaded = upload_to_aws(file_path + local_file, bucket_name, local_file)
  
  if max_speakers != -1:
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat=audio_file_name.split('.')[1],
        LanguageCode='en-US',
        Settings = {'ShowSpeakerLabels': True,
                  'MaxSpeakerLabels': max_speakers
                  }
    )
  else: 
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat=audio_file_name.split('.')[1],
        LanguageCode='en-US',
        Settings = {'ShowSpeakerLabels': True
                  }
    )    
  
  while True:
    result = transcribe.get_transcription_job(TranscriptionJobName=job_name)
    if result['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
        break
  if result['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
    data = pd.read_json(result['TranscriptionJob']['Transcript']['TranscriptFileUri'])
    transcript = data['results'][2][0]['transcript']

  # For retriveing the Speaker wise dialogues from json
  labels = data['results']['speaker_labels']['segments']
  speaker_start_times={}
  
  for label in labels:
    for item in label['items']:
      speaker_start_times[item['start_time']] = item['speaker_label']

  items = data['results']['items']
  lines = []
  line = ''
  time = 0
  speaker = 'null'
  i = 0

  # loop through all elements
  for item in items:
    i = i+1
    content = item['alternatives'][0]['content']

    # if it's starting time
    if item.get('start_time'):
      current_speaker = speaker_start_times[item['start_time']]
    
    # in AWS output, there are types as punctuation
    elif item['type'] == 'punctuation':
      line = line + content

    # handle different speaker
    if current_speaker != speaker:
      if speaker and speaker != 'null':
        if speaker == 'spk_1':
          lines.append({'speaker':'Agent', 'line':line, 'time':time})

        elif speaker == 'spk_0':
          lines.append({'speaker':'Customer', 'line':line, 'time':time}) 

      line = content
      speaker = current_speaker
      time = item['start_time']
      
    elif item['type'] != 'punctuation':
      line = line + ' ' + content
  if speaker == 'spk_1':
    lines.append({'speaker':'Agent', 'line':line, 'time':time})

  elif speaker == 'spk_0':
    lines.append({'speaker':'Customer', 'line':line, 'time':time}) 

  # sort the results by the time
  sorted_lines = sorted(lines,key=lambda k: float(k['time']))

  # write into the .txt file
  audio_file_name = (audio_file_name).split('.')[0]
  with open(file_path + audio_file_name+'.txt','w') as w:
    for line_data in sorted_lines:
      line = line_data.get('speaker') + ': ' + line_data.get('line')
      w.write(line + '\n')

  return data, result, transcript


if __name__ == "__main__":

  start = time.time()
  data, result, transcript = amazon_transcribe(local_file, 2)
  print(transcript)
  print("Time Taken:", (time.time() - start))

